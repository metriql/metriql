package com.metriql.warehouse.spi.bridge

import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.ReportMetric.ReportMeasure
import com.metriql.service.jinja.MetriqlJinjaContext
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.DimensionName
import com.metriql.service.model.MeasureName
import com.metriql.service.model.Model
import com.metriql.service.model.Model.Measure.AggregationType
import com.metriql.service.model.Model.Measure.AggregationType.AVERAGE
import com.metriql.service.model.Model.Measure.AggregationType.AVERAGE_DISTINCT
import com.metriql.service.model.Model.Measure.AggregationType.SQL
import com.metriql.service.model.Model.Measure.AggregationType.COUNT
import com.metriql.service.model.Model.Measure.AggregationType.COUNT_UNIQUE
import com.metriql.service.model.Model.Measure.AggregationType.MAXIMUM
import com.metriql.service.model.Model.Measure.AggregationType.MINIMUM
import com.metriql.service.model.Model.Measure.AggregationType.SUM
import com.metriql.service.model.Model.Measure.AggregationType.SUM_DISTINCT
import com.metriql.service.model.Model.Relation.RelationType.MANY_TO_MANY
import com.metriql.service.model.Model.Relation.RelationType.MANY_TO_ONE
import com.metriql.service.model.Model.Relation.RelationType.ONE_TO_MANY
import com.metriql.service.model.ModelDimension
import com.metriql.service.model.ModelName
import com.metriql.service.model.ModelRelation
import com.metriql.service.model.RelationName
import com.metriql.util.DefaultJinja
import com.metriql.util.MetriqlException
import com.metriql.util.ValidationUtil
import com.metriql.util.ValidationUtil.stripLiteral
import com.metriql.util.serializableName
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.ADHOC
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_ACCUMULATE
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_MERGE
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.Companion.trinoVersion
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.MetricPositionType
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.RenderedFilter
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.filter.FilterOperator
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.spi.type.StandardTypes
import java.time.LocalDate

abstract class ANSISQLMetriqlBridge : WarehouseMetriqlBridge {

    override val quote = '"'
    override fun quoteIdentifier(identifier: String) = ValidationUtil.quoteIdentifier(identifier, quote)

    override val metricRenderHook = object : WarehouseMetriqlBridge.MetricRenderHook {}

    override val mqlTypeMap = mapOf(
        StandardTypes.BOOLEAN to "boolean",
        StandardTypes.VARCHAR to "varchar",
        StandardTypes.BIGINT to "bigint",
        StandardTypes.INTEGER to "integer",
        StandardTypes.SMALLINT to "smallint",
        StandardTypes.DATE to "date",
        StandardTypes.DECIMAL to "decimal",
        StandardTypes.DOUBLE to "double",
        StandardTypes.TIMESTAMP to "timestamp",
        StandardTypes.TIME to "time",
        StandardTypes.CHAR to "char",
    )

    override val functions = mapOf(
        RFunction.NOW to "CURRENT_TIMESTAMP",
        RFunction.SUBSTRING to "SUBSTRING({{value[0]}}, {{value[1]}}, {{value[2]}})",
        RFunction.CEIL to "CEIL({{value[0]}}, )",
        RFunction.FLOOR to "FLOOR({{value[0]}})",
        RFunction.ROUND to "FLOOR({{value[0]}})",
        RFunction.VERSION to "'${stripLiteral(trinoVersion)}' as _col1",
    )

    override val supportedDBTTypes: Set<DBTType> = setOf()
    override val supportedJoins: Set<Model.Relation.JoinType> = setOf(
        Model.Relation.JoinType.FULL_JOIN,
        Model.Relation.JoinType.INNER_JOIN,
        Model.Relation.JoinType.LEFT_JOIN,
        Model.Relation.JoinType.RIGHT_JOIN
    )

    override fun renderFilter(
        filter: ReportFilter,
        contextModelName: ModelName,
        context: IQueryGeneratorContext
    ): RenderedFilter {
        return when (filter.value) {
            is ReportFilter.FilterValue.Sql -> {
                val renderedQuery = context.renderSQL(filter.value.sql, contextModelName)
                RenderedFilter(listOf(), renderedQuery, null)
            }
            is ReportFilter.FilterValue.MetricFilter -> {
                val joins = mutableListOf<String>()
                val wheres = mutableListOf<String>()
                val havings = mutableListOf<String>()

                filter.value.filters.forEach {
                    val metricValue = it.metricValue ?: filter.value.metricValue ?: throw java.lang.IllegalStateException()
                    when (metricValue) {
                        is ReportMetric.ReportDimension -> {
                            // hacky way to find out relation if the dimension doesn't belong to context model
                            val relationName = if (contextModelName != metricValue.modelName) {
                                context.referencedRelations
                                    .filter { it.value.sourceModelName == contextModelName && it.value.targetModelName == metricValue.modelName }
                                    .map { it.value }
                                    .firstOrNull()?.relation?.name

                                // TODO: find out if the dimension is from the parent model before throwing exception
                                // the filter is not applicable to this model
//                                throw MetriqlException("Dimension is not applicable for context model: $metricValue", HttpResponseStatus.BAD_REQUEST)
                                null
                            } else metricValue.relationName

                            val renderedMetric = renderDimension(
                                context,
                                contextModelName,
                                metricValue.name,
                                relationName,
                                metricValue.postOperation,
                                MetricPositionType.FILTER
                            )

                            val value = if (it.value is SQLRenderable) {
                                context.renderSQL(it.value, contextModelName)
                            } else {
                                it.value
                            }

                            if (renderedMetric.join != null) {
                                joins.add(renderedMetric.join)
                            }

                            wheres.add(
                                filters.generateFilter(
                                    context,
                                    it.operator as FilterOperator,
                                    renderedMetric.value,
                                    value
                                )
                            )
                        }
                        is ReportMetric.ReportMappingDimension -> {
                            val mappingType = metricValue.name
                            val postOperation = metricValue.postOperation
                            val dimensionName = context.getMappingDimensions(contextModelName).get(mappingType)
                                ?: throw MetriqlException("'${mappingType.serializableName}' mapping type not found for model $contextModelName", HttpResponseStatus.BAD_REQUEST)
                            val renderedMetric = renderDimension(
                                context,
                                contextModelName,
                                dimensionName,
                                null,
                                postOperation,
                                MetricPositionType.FILTER
                            )
                            wheres.add(
                                filters.generateFilter(
                                    context,
                                    it.operator as FilterOperator,
                                    renderedMetric.value,
                                    it.value
                                )
                            )
                        }
                        is ReportMeasure -> {
                            val renderedMetric = renderMeasure(
                                context,
                                contextModelName,
                                metricValue.name,
                                metricValue.relationName,
                                MetricPositionType.FILTER,
                                ADHOC
                            )
                            val havingFilters = filter.value.filters.joinToString(" OR ") { measureFilter ->
                                filters.generateFilter(
                                    context,
                                    measureFilter.operator as FilterOperator,
                                    renderedMetric.value,
                                    measureFilter.value
                                )
                            }

                            if (renderedMetric.join != null) {
                                joins.add(renderedMetric.join)
                            }

                            havings.add(havingFilters)
                        }
                        is ReportMetric.Function -> TODO()
                        is ReportMetric.Unary -> TODO()
                    }
                }

                val whereFilters = wheres.joinToString(" OR ")
                val havingFilters = havings.joinToString(" OR ")

                RenderedFilter(
                    joins,
                    if (wheres.size > 1) "($whereFilters)" else if (wheres.size == 1) whereFilters else null,
                    if (havings.size > 1) "($havingFilters)" else if (havings.size == 1) havingFilters else null
                )
            }
        }
    }

    override fun performAggregation(columnValue: String, aggregationType: AggregationType, context: WarehouseMetriqlBridge.AggregationContext): String {
        return when (aggregationType) {
            // these types use the same function in all aggregation context
            MAXIMUM -> "max($columnValue)"
            MINIMUM -> "min($columnValue)"
            SUM -> "sum($columnValue)"
            COUNT -> when (context) {
                ADHOC -> "count($columnValue)"
                INTERMEDIATE_ACCUMULATE -> "count($columnValue)"
                INTERMEDIATE_MERGE -> "coalesce(sum($columnValue), 0)"
            }
            COUNT_UNIQUE -> when (context) {
                ADHOC -> "count(distinct $columnValue)"
                else -> throw MetriqlException("`countUnique` aggregation can't be used in `aggregate`, please use `approximateUnique`", HttpResponseStatus.BAD_REQUEST)
            }
            AVERAGE -> when (context) {
                ADHOC -> "avg($columnValue)"
                else -> null
            }
            SUM_DISTINCT -> when (context) {
                ADHOC -> "sum(distinct $columnValue)"
                else -> null
            }
            AVERAGE_DISTINCT -> when (context) {
                ADHOC -> "avg(distinct $columnValue)"
                else -> null
            }
            SQL -> columnValue
            else -> null
        } ?: throw MetriqlException("`$aggregationType` measure is not supported for $context(`$columnValue`)", HttpResponseStatus.BAD_REQUEST)
    }

    override fun renderMeasure(
        context: IQueryGeneratorContext,
        contextModelName: ModelName,
        measureName: MeasureName,
        relationName: RelationName?,
        metricPositionType: MetricPositionType,
        queryType: WarehouseMetriqlBridge.AggregationContext,
        extraFilters: List<ReportFilter>?,
    ): WarehouseMetriqlBridge.RenderedField {
        val (modelMeasure, modelRelation) = if (relationName != null) {
            if (contextModelName == null) {
                throw IllegalStateException("Context model name is required when measure has a relation to join")
            }
            val modelRelation = context.getRelation(contextModelName, relationName)
            // Get the dimensions model-name from joins target model
            val modelMeasure = context.getModelMeasure(measureName, modelRelation.targetModelName)
            Pair(modelMeasure, modelRelation)
        } else {
            val modelMeasure = context.getModelMeasure(measureName, contextModelName)
            Pair(modelMeasure, null)
        }

        val measure = modelMeasure.measure
        val isWindow = measure.value is Model.Measure.MeasureValue.Sql && measure.value.window == true

        val filters = (modelMeasure.measure.filters ?: listOf()) + (extraFilters ?: listOf())
        val isFilterInPushdown = measure.type == Model.Measure.Type.SQL && measure.value.agg == null

        val rawValue = when (measure.value) {
            // Exclude aggregation if filter is present. Aggregation function will be added later
            is Model.Measure.MeasureValue.Column -> {
                when (queryType) {
                    ADHOC, INTERMEDIATE_ACCUMULATE ->
                        if (measure.value.column == null)
                            "*" else
                            context.getSQLReference(modelMeasure.target, modelMeasure.modelName, measure.value.column)
                    INTERMEDIATE_MERGE -> quoteIdentifier(measureName)
                }
            }
            is Model.Measure.MeasureValue.Sql -> {
                when (queryType) {
                    ADHOC, INTERMEDIATE_ACCUMULATE -> {
                        // If there is no aggregation, we need to pushdown the filters to underlying measures
                        val hook: ((Map<String, Any?>) -> Map<String, Any?>)? = if (isFilterInPushdown) {
                            {
                                (it["measure"] as MetriqlJinjaContext.MeasureContext).pushdownFilters = filters
                                it
                            }
                        } else null

                        context.renderSQL(
                            measure.value.sql, modelMeasure.modelName,
                            hook = hook,
                            renderAlias = measure.value.window != null
                        )
                    }
                    INTERMEDIATE_MERGE -> quoteIdentifier(measureName)
                }
            }
            is Model.Measure.MeasureValue.Dimension -> {
                val sqlMeasure = Model.Measure.MeasureValue.Sql("{{dimension.${measure.value.dimension}}}", measure.value.aggregation)

                when (queryType) {
                    ADHOC, INTERMEDIATE_ACCUMULATE -> context.renderSQL(sqlMeasure.sql, modelMeasure.modelName)
                    INTERMEDIATE_MERGE -> quoteIdentifier(measureName)
                }
            }
        }

        val baseJoinRelations = if (modelRelation != null) {
            generateJoinStatement(modelRelation, context)
        } else null

        // A filter might use a dimension from a relation. New joins may be added.
        // If the aggregation is not set, the only use case is it's a SQL
        val (value, joinRelations) = if (filters.isNotEmpty() && !isFilterInPushdown) {
            if (contextModelName == null) {
                throw MetriqlException(
                    "Measure '${measure.name}' can't be rendered in this context while it uses relational dimensions in filters and context model name is not set.",
                    HttpResponseStatus.BAD_REQUEST
                )
            }
            val renderedFilters = filters.map {
                // Measure filters only have metric filter
                val metricValue = (it.value as ReportFilter.FilterValue.MetricFilter).metricValue
                val filterDimensionsModelName = if (metricValue is ReportMetric.ReportDimension) {
                    metricValue.modelName!!
                } else {
                    contextModelName
                }
                renderFilter(
                    it,
                    filterDimensionsModelName,
                    context
                )
            }

            val measureFilterExpression = renderedFilters
                .mapNotNull { it.whereFilter }
                .joinToString(" AND ") { "($it)" }

            val measureJoinsFromFilter = renderedFilters
                .flatMap { it.joins }
                .joinToString("\n") { it }

            val columnValue = "CASE WHEN $measureFilterExpression THEN $rawValue ELSE NULL END"
            // Check if report-measure has a relation-name. Join that relation if present.

            val combinedJoinRelations = listOf(baseJoinRelations, measureJoinsFromFilter)
                .mapNotNull { it }
                .joinToString("\n") { it }
            columnValue to combinedJoinRelations
        } else {
            rawValue to baseJoinRelations
        }

        val renderedValue = if (measure.value.agg != null) {
            val nonSymmetricAggregates = context.referencedRelations.filter {
                it.value.relation.relationType == ONE_TO_MANY ||
                    it.value.relation.relationType == MANY_TO_MANY ||
                    (relationName != null && it.value.relation.relationType == MANY_TO_ONE)
            }
            val (aggregation, nonSymmetricAggregatedValue) = if (nonSymmetricAggregates.isNotEmpty()) {
                val primaryKeyDimension = context.getModel(contextModelName)?.dimensions.find { it.primary == true }?.name
                    ?: throw MetriqlException("Primary key dimension is required for non-symmetric aggregates in `$contextModelName`", HttpResponseStatus.BAD_REQUEST)

                val primaryKey = renderDimension(context, contextModelName, primaryKeyDimension, null, null, MetricPositionType.FILTER)

                val (aggregation, nullableNonSymmetricAggregatedValue) = when (measure.value.agg) {
                    AVERAGE -> null to nonSymmetricAggregateAvg(value, primaryKey.value)
                    SUM -> null to nonSymmetricAggregateSum(value, primaryKey.value)
                    COUNT -> COUNT_UNIQUE to if (value == "*") primaryKey.value else "CASE WHEN $value IS NOT NULL THEN ${primaryKey.value} ELSE NULL END"
                    else -> measure.value.agg to value
                }

                if (nullableNonSymmetricAggregatedValue == null) {

                    throw MetriqlException(
                        """
                    Non-symmetric aggregation detected, this can show unexpected results so it's disabled by default.
                    Aggregation '${measure.value.agg.serializableName}'
                    ${nonSymmetricAggregates.map { "`${it.value.relation.name}` relation from model `${it.value.sourceModelName}`" }}
                        """.trimIndent(),
                        HttpResponseStatus.BAD_REQUEST
                    )
                }

                aggregation to nullableNonSymmetricAggregatedValue
            } else {
                measure.value.agg to value
            }

            if (aggregation != null) {
                performAggregation(nonSymmetricAggregatedValue, aggregation, queryType)
            } else {
                nonSymmetricAggregatedValue
            }
        } else {
            value
        }

        val alias = context.getMeasureAlias(measure.name, relationName)

        val measureValueForPositionType = when (metricPositionType) {
            MetricPositionType.FILTER -> renderedValue
            MetricPositionType.PROJECTION -> "$renderedValue AS $alias"
        }

        return WarehouseMetriqlBridge.RenderedField(measureValueForPositionType, joinRelations, isWindow, alias)
    }

    open fun nonSymmetricAggregateSum(sql: String, primaryKey: String): String? {
        val functionDefinition = functions[RFunction.HEX_TO_INT] ?: return null
        val left = DefaultJinja.renderFunction(functionDefinition, listOf("LEFT(MD5(CAST( $primaryKey AS VARCHAR)),15)"))
        val right = DefaultJinja.renderFunction(functionDefinition, listOf("RIGHT(MD5(CAST( $primaryKey AS VARCHAR)),15)"))
        return """COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( $sql,0)*(1000000*1.0)) AS DECIMAL(38,0))) + 
            |CAST($left AS DECIMAL(38,0)) * 1.0e8 + 
            |CAST($right AS DECIMAL(38,0)) ) - 
            |SUM(DISTINCT CAST($left AS DECIMAL(38,0)) * 1.0e8 + 
            |CAST($right AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) /
            | CAST((1000000*1.0) AS DOUBLE PRECISION), 0)""".trimMargin()
    }

    open fun nonSymmetricAggregateAvg(sql: String, primaryKey: String): String? {
        val sum = nonSymmetricAggregateSum(sql, primaryKey) ?: return null
        return "$sum / NULLIF(COUNT(DISTINCT CASE WHEN $sql IS NOT NULL THEN $primaryKey  ELSE NULL END), 0)"
    }

    override fun generateDimensionMetaQuery(
        context: IQueryGeneratorContext,
        modelName: ModelName,
        modelTarget: Model.Target,
        dimensions: List<Model.Dimension>,
    ): String {
        val modelTargetSQL = context.getSQLReference(
            modelTarget,
            modelName,
            null,
            dimensions.map { it.name },
            DateRange(LocalDate.now().minusDays(14), LocalDate.now()) // Dimension discovery filter on last 14 days. Especially helpful for bigquery
        )

        val projectedDimensions = if (dimensions.isNotEmpty()) {
            dimensions
                .joinToString(", ") { dimension ->
                    val value = when (dimension.value) {
                        is Model.Dimension.DimensionValue.Column -> context.getSQLReference(modelTarget, modelName, dimension.value.column)
                        is Model.Dimension.DimensionValue.Sql -> context.renderSQL(dimension.value.sql, modelName)
                    }
                    "$value AS ${quoteIdentifier(context.getDimensionAlias(dimension.name, null, null))}"
                }
        } else {
            "*"
        }
        val rawSQLQuery = "SELECT $projectedDimensions FROM $modelTargetSQL"
        return generateQuery(context.viewModels, rawSQLQuery)
    }

    private fun generateModelDimension(
        contextModelName: ModelName,
        dimensionName: DimensionName,
        relationName: RelationName?,
        context: IQueryGeneratorContext,
    ): Pair<ModelDimension, ModelRelation?> {
        return if (relationName != null) {
            val modelRelation = context.getRelation(contextModelName, relationName)
            // Get the dimensions model-name from joins target model
            val modelDimension = context.getModelDimension(dimensionName, modelRelation.targetModelName)
            Pair(modelDimension, modelRelation)
        } else {
            val modelDimension = context.getModelDimension(dimensionName, contextModelName)
            Pair(modelDimension, null)
        }
    }

    override fun renderDimension(
        context: IQueryGeneratorContext,
        contextModelName: ModelName,
        dimensionName: DimensionName,
        relationName: RelationName?,
        postOperation: ReportMetric.PostOperation?,
        metricPositionType: MetricPositionType,
    ): WarehouseMetriqlBridge.RenderedField {
        val (modelDimension, modelRelation) = generateModelDimension(contextModelName, dimensionName, relationName, context)

        val dimension = modelDimension.dimension
        val isWindow = dimension.value is Model.Dimension.DimensionValue.Sql && dimension.value.window == true

        val rawValue = when (dimension.value) {
            is Model.Dimension.DimensionValue.Column -> context.getSQLReference(modelDimension.target, modelDimension.modelName, dimension.value.column)
            is Model.Dimension.DimensionValue.Sql -> context.renderSQL(dimension.value.sql, modelDimension.modelName, renderAlias = dimension.value.window != null)
        }

        val value = metricRenderHook.dimensionBeforePostOperation(context, metricPositionType, dimension, postOperation, rawValue)

        val postProcessedDimension = if (postOperation != null) { // && modelDimension.dimension.fieldType != null
            val template = when (postOperation.type) {
                ReportMetric.PostOperation.Type.TIMESTAMP -> timeframes.timestampPostOperations[postOperation.value]
                ReportMetric.PostOperation.Type.DATE -> timeframes.datePostOperations[postOperation.value]
                ReportMetric.PostOperation.Type.TIME -> timeframes.timePostOperations[postOperation.value]
            } ?: throw MetriqlException("Post operation is not supported", HttpResponseStatus.BAD_REQUEST)
            metricRenderHook.dimensionAfterPostOperation(context, metricPositionType, dimension, postOperation, String.format(template, value))
        } else {
            value
        }

        val joinRelations = if (modelRelation != null) generateJoinStatement(modelRelation, context) else null

        val alias = context.getDimensionAlias(dimensionName, relationName, postOperation)
        val dimensionValue = when (metricPositionType) {
            MetricPositionType.FILTER -> postProcessedDimension
            MetricPositionType.PROJECTION -> "$postProcessedDimension AS ${quoteIdentifier(alias)}"
        }

        return WarehouseMetriqlBridge.RenderedField(dimensionValue, joinRelations, isWindow, alias)
    }

    override fun generateJoinStatement(
        modelRelation: ModelRelation,
        context: IQueryGeneratorContext,
    ): String {
        val relation = modelRelation.relation
        val joinType = when (relation.joinType) {
            Model.Relation.JoinType.LEFT_JOIN -> "LEFT JOIN"
            Model.Relation.JoinType.INNER_JOIN -> "INNER JOIN"
            Model.Relation.JoinType.RIGHT_JOIN -> "RIGHT JOIN"
            Model.Relation.JoinType.FULL_JOIN -> "FULL JOIN"
        }
        val joinModel = context.getSQLReference(modelRelation.targetModelTarget, modelRelation.targetModelName, null)
        val joinExpression = when (relation.value) {
            is Model.Relation.RelationValue.SqlValue -> {
                context.renderSQL(relation.value.sql, modelRelation.sourceModelName, targetModelName = modelRelation.targetModelName)
            }
            is Model.Relation.RelationValue.ColumnValue -> {
                val columnTypeRelation = relation.value
                "${
                    context.getSQLReference(
                        modelRelation.sourceModelTarget,
                        modelRelation.sourceModelName,
                        columnTypeRelation.sourceColumn
                    )
                } = ${
                    context.getSQLReference(
                        modelRelation.targetModelTarget,
                        modelRelation.targetModelName,
                        columnTypeRelation.targetColumn
                    )
                }"
            }
            is Model.Relation.RelationValue.DimensionValue -> {
                "${
                    renderDimension(
                        context,
                        modelRelation.sourceModelName,
                        relation.value.sourceDimension,
                        null,
                        null,
                        MetricPositionType.FILTER
                    ).value
                } = ${
                    renderDimension(
                        context,
                        modelRelation.targetModelName,
                        relation.value.targetDimension,
                        null,
                        null,
                        MetricPositionType.FILTER
                    ).value
                }"
            }
        }
        return "$joinType $joinModel ON ($joinExpression)"
    }

    // Append viewModels as WITH model AS (), model_other as ()
    override fun generateQuery(viewModels: Map<ModelName, String>, rawQuery: String, comments: List<String>): String {
        val comments = if (comments.isEmpty()) "" else
            """/*
${comments.joinToString("\n") { "   * $it" }}
*/

            """.trimIndent()

        return comments + (
            if (viewModels?.isNotEmpty()) {
                val views = viewModels.map { (modelName, sql) ->
                    "${quoteIdentifier(modelName)} AS (\n$sql\n)"
                }.joinToString(",\n")
                "WITH $views \n${rawQuery.replace(multiLineRegex, "\n")} \n"
            } else {
                rawQuery
            }
            )
    }

    companion object {
        private val multiLineRegex = "\\n+".toRegex()
    }
}
