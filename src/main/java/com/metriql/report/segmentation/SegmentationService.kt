package com.metriql.report.segmentation

import com.metriql.db.FieldType
import com.metriql.report.IAdHocService
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportFilter.Companion.extractDateRangeForEventTimestamp
import com.metriql.report.data.ReportMetric.ReportDimension
import com.metriql.report.data.ReportMetric.ReportMeasure
import com.metriql.report.segmentation.SegmentationQueryReWriter.MaterializeTableCache
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.Model
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP
import com.metriql.service.model.Model.Target.TargetValue.Table
import com.metriql.service.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.ADHOC
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_ACCUMULATE
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_MERGE
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.segmentation.Segmentation
import com.metriql.warehouse.spi.services.segmentation.SegmentationQueryGenerator
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.Collections.newSetFromMap
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger

/*
* Segmentation service can render segmentation query and generate modelTarget for:
* Funnel & Retention: While rendering modelTargets with dimension we use this service
*   because we allow users to select grouping and filtering dimensions both on for the model itself
*   and dimensions from join relations. Using a single service to generate the model-target
*   drastically reduced the duplicate code.
*
* For example: A funnel reports, first step on model 'app_opened' and filters the country dimension,
* the country dimension is populated from a join relation on 'user_attributes' model.
* Instead of generating the join relation on funnel service, we apply the filters (thus generating the join relation if needed)
* using this service
* */
class SegmentationService : IAdHocService<SegmentationReportOptions> {
    // check if materialize exists in the database, if not, ignore it
    private val materializeTableExists = newSetFromMap(ConcurrentHashMap<MaterializeTableCache, Boolean>())

    override fun generateMaterializeQuery(
        projectId: String,
        context: IQueryGeneratorContext,
        modelName: ModelName,
        materalizeName: String,
        materialize: SegmentationRecipeQuery.SegmentationMaterialize,
    ): Pair<Table, String> {
        val model = context.getModel(modelName)
        val reportOptions = materialize.toQuery(modelName).toReportOptions(context) as SegmentationReportOptions
        val eventTimestamp = model.mappings.get(EVENT_TIMESTAMP)
        if (eventTimestamp != null) {
            // check if HOUR, DAY, etc.
            if (reportOptions.dimensions?.any { it.name == eventTimestamp && it.relationName == null } != true) {
                throw MetriqlException(
                    "For the aggregate '$materalizeName' in model '$modelName', " +
                        "the event timestamp ($eventTimestamp) with timeframe HOUR or DAY must be included.",
                    HttpResponseStatus.BAD_REQUEST
                )
            }
        }

        val (_, rawQuery, _) = renderQuery(context.auth, context, reportOptions, listOf(), useAggregate = false, forAccumulator = true)
        val (materializeTable, _, _) = renderQuery(context.auth, context, reportOptions, listOf(), useAggregate = true, forAccumulator = true)
        return Pair(materializeTable!!, rawQuery)
    }

    override fun getUsedModels(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: SegmentationReportOptions): Set<ModelName> {
        val filterRelations = (reportOptions.filters ?: listOf()).mapNotNull {
            when (it.value) {
                // TODO
                is ReportFilter.FilterValue.MetricFilter -> it.value.metricValue?.toMetricReference()?.relation
                is ReportFilter.FilterValue.Sql -> null
            }
        }

        val allRelations = (filterRelations + reportOptions.measures.mapNotNull { it.relationName } + (reportOptions.dimensions ?: listOf()).mapNotNull { it.relationName }).toSet()

        val model = context.getModel(reportOptions.modelName)
        val relationModels = allRelations.map { relation -> model.relations.first { it.name == relation }.modelName }.toSet()
        return relationModels + setOf(reportOptions.modelName)
    }

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: SegmentationReportOptions,
        reportFilters: List<ReportFilter>,
    ): IAdHocService.RenderedQuery {
        val (_, query, dsl) = renderQuery(
            auth,
            context,
            reportOptions,
            reportFilters,
            useAggregate = true
        )

        return IAdHocService.RenderedQuery(
            query,
            postProcessors = listOf { queryResult ->
                val metadata = queryResult.metadata?.mapIndexed { index, col -> col.copy(name = dsl.columnNames[index]) }
                queryResult.copy(metadata = metadata)
            }
        )
    }

    fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: SegmentationReportOptions,
        reportFilters: List<ReportFilter>,
        useAggregate: Boolean = false,
        forAccumulator: Boolean = false,
    ): Triple<Table?, String, Segmentation> {
        val queryGenerator = context.datasource.warehouse.bridge.queryGenerators[SegmentationReportType.slug]
        val serviceQueryGenerator = queryGenerator as? SegmentationQueryGenerator ?: throw IllegalArgumentException("Warehouse query generator must be SegmentationQueryGenerator")

        val (materializedTarget, dsl) = createDSL(auth, context.datasource, context, reportOptions, reportFilters, useAggregate, forAccumulator)

        val finalDsl = if (materializedTarget != null) {
//            val mainModel = context.getModel(reportOptions.modelName)
//            if (mainModel.target.value is Model.Target.TargetValue.Sql) {
//                val realtimeDsl = createDSL(auth, dataSource, context, reportOptions, listOf(), listOf(), useAggregate = false, forAccumulator = true)
//                val realtimeSql = serviceQueryGenerator.generateSQL(auth, context, realtimeDsl.second, reportOptions)
            // don't use *, instead include columns
//                dsl.copy(tableReference = "(SELECT * FROM ${dsl.tableReference} UNION ALL ($realtimeSql)) ${dsl.tableAlias}")
//            } else
            dsl
        } else dsl

        val query = serviceQueryGenerator.generateSQL(auth, context, finalDsl, reportOptions)
        return Triple(materializedTarget, query, dsl)
    }

    private fun createDSL(
        auth: ProjectAuth,
        dataSource: DataSource,
        context: IQueryGeneratorContext,
        reportOptions: SegmentationReportOptions,
        reportFilters: List<ReportFilter>,
        useAggregate: Boolean,
        forAccumulator: Boolean,
    ): Pair<Table?, Segmentation> {
        val warehouseBridge = dataSource.warehouse.bridge
        val mainModel = context.getModel(reportOptions.modelName)

        val aggregatesForModel = context.getAggregatesForModel(mainModel.target, SegmentationReportType)

        val usedModels = getUsedModels(auth, context, reportOptions)
        val alwaysFilters = usedModels.flatMap { model -> context.getModel(model).alwaysFilters?.map { it.toReportFilter(context, model) } ?: listOf() }

        val (materializeQuery, aggregateModel) = if (useAggregate) {
            val allFilters = (reportOptions.filters ?: listOf()) + reportFilters + alwaysFilters
            try {
                SegmentationQueryReWriter(context).findOptimumPlan(
                    reportOptions.copy(filters = allFilters), aggregatesForModel
                ) {
                    if (materializeTableExists.contains(it) || forAccumulator) {
                        true
                    } else {
                        try {
                            dataSource.getTableSchema(it.target.database, it.target.schema, it.target.table) != null
                            materializeTableExists.add(it)
                            true
                        } catch (e: MetriqlException) {
                            if (e.statusCode == HttpResponseStatus.NOT_FOUND) {
                                false
                            } else {
                                throw e
                            }
                        }
                    }
                } ?: Pair(null, null)
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Error building aggregate", e)
                Pair(null, null)
            }
        } else {
            Pair(null, null)
        }

        if (aggregateModel != null) {
            context.addModel(aggregateModel)
        }

        val queryType = if (aggregateModel != null) INTERMEDIATE_MERGE else if (forAccumulator) INTERMEDIATE_ACCUMULATE else ADHOC
        val contextModelTarget = aggregateModel?.target ?: mainModel.target
        val modelName = materializeQuery?.modelName ?: reportOptions.modelName
        val dimensions = materializeQuery?.dimensions ?: reportOptions.dimensions ?: listOf()
        val measures = materializeQuery?.measures ?: reportOptions.measures
        val filters = materializeQuery?.filters ?: reportOptions.filters ?: listOf()
        val orders = reportOptions.orders ?: listOf()
        val alias = context.getOrGenerateAlias(modelName, null)

        // 1. Render measures, dimensions and filters.
        // Each of them will generate a projection value and a join relation if available.
        val renderedDimensions = dimensions
            .map { dimension ->
                // Note that renderDimension takes the context as an argument.
                // We still keep track of each dimension rendered.
                warehouseBridge.renderDimension(
                    context,
                    dimension.modelName,
                    dimension.name,
                    dimension.relationName,
                    dimension.postOperation,
                    WarehouseMetriqlBridge.MetricPositionType.PROJECTION
                )
            }

        val modelAndQueryFilters = (filters + alwaysFilters)
        val allFilters = modelAndQueryFilters + reportFilters

        val renderedFilters = modelAndQueryFilters.map { warehouseBridge.renderFilter(it, modelName, context) } +
            reportFilters.mapNotNull {
                try {
                    warehouseBridge.renderFilter(it, modelName, context)
                } catch (e: MetriqlException) {
                    context.comments.add("Unable to apply report filter ${it.value}: ${e.message}")
                    // TODO: implement a fail safe mode?
                    null
                }
            }

        // Need to render measures at last and call the relations in advance to trigger symmetric aggregates if required.
        measures.filter { it.relationName != null }.forEach { context.getRelation(it.modelName, it.relationName!!) }

        val renderedMeasures = measures
            .map { measure ->
                warehouseBridge.renderMeasure(
                    context,
                    measure.modelName,
                    measure.name,
                    measure.relationName,
                    WarehouseMetriqlBridge.MetricPositionType.PROJECTION,
                    queryType,
                )
            }

        val joinRelations = (renderedFilters.mapNotNull { it.joins }.flatten() + (renderedDimensions.mapNotNull { it.join } + renderedMeasures.mapNotNull { it.join })).toSet()

        /*
        * 2. Prepare parts.
        * a) Projections (dimensions + measures). SELECT a, sum(b)...
        * b) Table Reference: FROM table as modelName
        * c) Joins: LEFT JOIN x ON (..)
        * d) Where Filters: WHERE (X OR Y) AND (Z OR C) AND ...
        * e) Group: GROUP BY 1, 2
        * f) Having Filters: HAVING sum(x) > 50 AND avg(c) = 10.2
        * g) Order: ORDER BY 1
        * */

        // Render table reference last. Have to wait for the queryGeneratorContext to fill with rendered dimensions
        val renderedDimensionAndColumnNames = context.referencedDimensions
            .filter { it.value.modelName == modelName }
            .map { it.value.dimension.name }

        val inQueryDimensionNames = dimensions.map { it.name } + renderedDimensionAndColumnNames

        val dsl = Segmentation(
            columnNames = getColumnNames(context, mainModel, dimensions, measures),
            dimensions = renderedDimensions,
            limit = reportOptions.limit,
            measures = renderedMeasures,
            whereFilters = renderedFilters.mapNotNull { it.whereFilter },

            groupIdx = if (renderedMeasures.any { !it.window } || renderedFilters.any { it.havingFilter != null }) {
                (1..renderedDimensions.filter { !it.window }.size).toSet()
            } else null,

            groups = if (dimensions.isNotEmpty()) {
                dimensions.map { reportDimension ->
                    warehouseBridge.renderDimension(
                        context,
                        modelName,
                        reportDimension.name,
                        reportDimension.relationName,
                        reportDimension.postOperation,
                        WarehouseMetriqlBridge.MetricPositionType.FILTER
                    ).value
                }.toSet()
            } else null,

            havingFilters = renderedFilters.mapNotNull { it.havingFilter }.toSet(),

            orderByIdx = when {
                forAccumulator -> null
                orders.isNotEmpty() -> {
                    orders.map { orderItem ->
                        val metricIndex = when (orderItem.value) {
                            is ReportDimension -> {
                                val index = dimensions.indexOf(orderItem.value)
                                if (index == -1) {
                                    // the order by is not visible in the query
                                    throw MetriqlException("The order by must be one of the dimensions", HttpResponseStatus.BAD_REQUEST)
                                } else {
                                    // sql starts from 1 index
                                    index + 1
                                }
                            }
                            is ReportMeasure -> {
                                (dimensions.size + measures.indexOf(orderItem.value)) + 1
                            }
                            else -> throw IllegalStateException("Only dimension and measure are accepted as segmentation order")
                        }
                        "$metricIndex ${if (orderItem.ascending == true) "ASC" else "DESC"}"
                    }.toSet()
                }
                else -> null
            },

            orderBy = if (forAccumulator) null else if (orders.isNotEmpty()) {
                orders.map { orderItem ->
                    val metricIndex = when (orderItem.value) {
                        is ReportDimension -> {
                            warehouseBridge.renderDimension(
                                context,
                                modelName,
                                orderItem.value.name,
                                orderItem.value.relationName,
                                orderItem.value.postOperation,
                                WarehouseMetriqlBridge.MetricPositionType.FILTER
                            ).value
                        }
                        is ReportMeasure -> {
                            warehouseBridge.renderMeasure(
                                context,
                                orderItem.value.modelName,
                                orderItem.value.name,
                                orderItem.value.relationName,
                                WarehouseMetriqlBridge.MetricPositionType.FILTER,
                                queryType,
                            ).value
                        }
                        else -> throw IllegalStateException("Only ReportDimension and ReportMeasure are accepted as segmentation order")
                    }
                    "$metricIndex ${if (orderItem.ascending == true) "ASC" else "DESC"}"
                }.toSet()
            } else {
                if (measures.isNotEmpty()) {
                    val timestampDimensionsIndexes = dimensions?.mapIndexed { index, item ->
                        val currentModelName = if (item.relationName != null) {
                            context.getModel(item.modelName).relations.find { it.name == item.relationName }?.modelName
                                ?: throw MetriqlException(HttpResponseStatus.NOT_FOUND)
                        } else {
                            item.modelName ?: modelName
                        }
                        val modelDimension = context.getModelDimension(item.name, currentModelName)
                        if (listOf(FieldType.TIMESTAMP, FieldType.DATE, FieldType.TIME).contains(modelDimension.dimension.fieldType)) {
                            index
                        } else {
                            null
                        }
                    }?.mapNotNull { it }

                    val firstMeasure = "${dimensions.size + 1} DESC"

                    if (timestampDimensionsIndexes?.isNotEmpty()) {
                        timestampDimensionsIndexes.map { "${it + 1} DESC" } + firstMeasure
                    } else {
                        listOf(firstMeasure)
                    }.toSet()
                } else null
            },

            // Join other rendered relations for this model, some relations are only generated inside the jinja context; join through relations.
            joins = (
                joinRelations + context.referencedRelations
                    .filter { it.key.first == modelName } // Take only relations that are targeted to this model.
                    .values.map {
                        warehouseBridge.generateJoinStatement(context, it)
                    }
                ).reversed().toSet(),
            tableReference = context.getSQLReference(
                contextModelTarget, alias, modelName, null, if (forAccumulator) null else inQueryDimensionNames,
                extractDateRangeForEventTimestamp(allFilters)
            ),
            tableAlias = alias
        )

        return aggregateModel?.target?.value as? Table to dsl
    }

    private fun getColumnNames(context: IQueryGeneratorContext, mainModel: Model, dimensions: List<ReportDimension>, measures: List<ReportMeasure>): List<String> {
        val dimensionNames = dimensions.map {
            val modelName = if (it.relationName == null) it.modelName else {
                mainModel.relations.find { relation -> relation.name == it.relationName }!!.modelName
            }
            context.getModelDimension(it.name, modelName).dimension
        }.map { it.label ?: it.name }

        val measureNames = measures.map {
            val modelName = if (it.relationName == null) it.modelName else {
                mainModel.relations.find { relation -> relation.name == it.relationName }!!.modelName
            }
            context.getModelMeasure(it.name, modelName).measure
        }.map { it.label ?: it.name }

        return dimensionNames + measureNames
    }

    private val logger = Logger.getLogger(this::class.java.name)
}
