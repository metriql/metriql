package com.metriql.warehouse.spi.bridge

import com.metriql.db.FieldType
import com.metriql.report.data.FilterValue
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.ReportMetric.ReportMeasure
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.Dataset.Measure.AggregationType
import com.metriql.service.dataset.Dataset.Measure.AggregationType.AVERAGE
import com.metriql.service.dataset.Dataset.Measure.AggregationType.AVERAGE_DISTINCT
import com.metriql.service.dataset.Dataset.Measure.AggregationType.COUNT
import com.metriql.service.dataset.Dataset.Measure.AggregationType.COUNT_UNIQUE
import com.metriql.service.dataset.Dataset.Measure.AggregationType.MAXIMUM
import com.metriql.service.dataset.Dataset.Measure.AggregationType.MINIMUM
import com.metriql.service.dataset.Dataset.Measure.AggregationType.SQL
import com.metriql.service.dataset.Dataset.Measure.AggregationType.SUM
import com.metriql.service.dataset.Dataset.Measure.AggregationType.SUM_DISTINCT
import com.metriql.service.dataset.Dataset.Relation.RelationType.MANY_TO_MANY
import com.metriql.service.dataset.Dataset.Relation.RelationType.MANY_TO_ONE
import com.metriql.service.dataset.Dataset.Relation.RelationType.ONE_TO_MANY
import com.metriql.service.dataset.DatasetName
import com.metriql.service.dataset.DimensionName
import com.metriql.service.dataset.MeasureName
import com.metriql.service.dataset.ModelDimension
import com.metriql.service.dataset.ModelRelation
import com.metriql.service.dataset.RelationName
import com.metriql.service.jinja.MetriqlJinjaContext
import com.metriql.service.jinja.SQLRenderable
import com.metriql.util.DefaultJinja
import com.metriql.util.MetriqlException
import com.metriql.util.ValidationUtil
import com.metriql.util.ValidationUtil.stripLiteral
import com.metriql.util.getOperation
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
        RFunction.CEIL to "CEIL({{value[0]}})",
        RFunction.FLOOR to "FLOOR({{value[0]}})",
        RFunction.ROUND to "ROUND({{value[0]}})",
        RFunction.UPPER to "UPPER({{value[0]}})",
        RFunction.LOWER to "LOWER({{value[0]}})",
        RFunction.DATE_TRUNC to "DATE_TRUNC({{value[0]}}, {{value[1]}})",
        RFunction.VERSION to "'${stripLiteral(trinoVersion)}' as _col1",
        RFunction.FROM_UNIXTIME to "to_timestamp({{value[0]}})"
    )

    override val supportedDBTTypes: Set<DBTType> = setOf()
    override val supportedJoins: Set<Dataset.Relation.JoinType> = setOf(
        Dataset.Relation.JoinType.FULL_JOIN,
        Dataset.Relation.JoinType.INNER_JOIN,
        Dataset.Relation.JoinType.LEFT_JOIN,
        Dataset.Relation.JoinType.RIGHT_JOIN
    )

    override fun renderFilter(
        filter: FilterValue,
        contextDatasetName: DatasetName,
        context: IQueryGeneratorContext
    ): RenderedFilter {
        return when (filter) {
            is FilterValue.NestedFilter -> {
                val renderedSubFilters = filter.filters.map { renderFilter(it, contextDatasetName, context) }
                val whereFilter = renderedSubFilters.mapNotNull { it.whereFilter }.joinToString(" ${filter.connector} ")
                RenderedFilter(
                    renderedSubFilters.flatMap { it.joins },
                    if (renderedSubFilters.size > 1) "($whereFilter)" else whereFilter,
                    renderedSubFilters.mapNotNull { it.havingFilter }.joinToString(" ${filter.connector} ")
                )
            }

            is FilterValue.SqlFilter -> {
                val renderedQuery = context.renderSQL(filter.sql, context.getOrGenerateAlias(contextDatasetName, null), contextDatasetName)
                RenderedFilter(listOf(), renderedQuery, null)
            }

            is FilterValue.MetricFilter -> {
                val joins = mutableListOf<String>()
                var wheres: String? = null
                var havings: String? = null

                val (clazz, type) = filter.metric.getType(context, contextDatasetName)
                when (clazz) {
                    ReportMetric.ReportDimension::class -> {

                        val dimension = filter.metric.toDimension(contextDatasetName, type)

                        val (dim, _) = generateModelDimension(contextDatasetName, filter.metric.name, filter.metric.relation, context)

                        val renderedMetric = renderDimension(
                            context,
                            contextDatasetName,
                            dimension.name,
                            dimension.relation,
                            dimension.timeframe,
                            MetricPositionType.FILTER
                        )

                        val value = if (filter.value is SQLRenderable) {
                            context.renderSQL(filter.value, context.getOrGenerateAlias(contextDatasetName, filter.metric.relation), contextDatasetName)
                        } else {
                            filter.value
                        }

                        if (renderedMetric.join != null) {
                            joins.add(renderedMetric.join)
                        }

                        val (_, operator) = getOperation(dimension.timeframe?.type?.fieldType ?: dim.dimension.fieldType, filter.operator)

                        wheres = filters.generateFilter(
                            context,
                            operator as FilterOperator,
                            renderedMetric.value,
                            value
                        )
                    }
                    ReportMeasure::class -> {
                        val renderedMetric = renderMeasure(
                            context,
                            contextDatasetName,
                            filter.metric.name,
                            filter.metric.relation,
                            MetricPositionType.FILTER,
                            ADHOC
                        )

                        val (_, operator) = getOperation(FieldType.DOUBLE, filter.operator)

                        havings = filters.generateFilter(
                            context,
                            operator as FilterOperator,
                            renderedMetric.value,
                            filter.value
                        )

                        if (renderedMetric.join != null) {
                            joins.add(renderedMetric.join)
                        }
                    }
                }

                RenderedFilter(
                    joins,
                    if (wheres != null) "$wheres" else null,
                    if (havings != null) "($havings)" else null
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
        contextDatasetName: DatasetName,
        measureName: MeasureName,
        relationName: RelationName?,
        metricPositionType: MetricPositionType,
        queryType: WarehouseMetriqlBridge.AggregationContext,
        extraFilters: List<FilterValue>?,
        modelAlias: String?
    ): WarehouseMetriqlBridge.RenderedField {
        val (modelMeasure, modelRelation) = if (relationName != null) {
            if (contextDatasetName == null) {
                throw IllegalStateException("Context model name is required when measure has a relation to join")
            }
            val modelRelation = context.getRelation(contextDatasetName, relationName)
            // Get the dimensions model-name from joins target model
            val modelMeasure = context.getModelMeasure(measureName, modelRelation.targetDatasetName)
            Pair(modelMeasure, modelRelation)
        } else {
            val modelMeasure = context.getModelMeasure(measureName, contextDatasetName)
            Pair(modelMeasure, null)
        }

        val measure = modelMeasure.measure
        val isWindow = measure.value is Dataset.Measure.MeasureValue.Sql && measure.value.window == true

        val filters = (modelMeasure.measure.filters ?: listOf()) + (extraFilters ?: listOf())
        val isFilterInPushdown = measure.type == Dataset.Measure.Type.SQL && measure.value.agg == null
        val alias = context.getOrGenerateAlias(contextDatasetName, relationName)

        val rawValue = when (measure.value) {
            // Exclude aggregation if filter is present. Aggregation function will be added later
            is Dataset.Measure.MeasureValue.Column -> {
                when (queryType) {
                    ADHOC, INTERMEDIATE_ACCUMULATE ->
                        if (measure.value.column == null)
                        // when filter is enabled, * fails
                            "1" else {
                            context.getSQLReference(modelMeasure.target, alias, modelMeasure.datasetName, measure.value.column)
                        }

                    INTERMEDIATE_MERGE -> quoteIdentifier(measureName)
                }
            }

            is Dataset.Measure.MeasureValue.Sql -> {
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
                            measure.value.sql,
                            alias,
                            modelMeasure.datasetName,
                            hook = hook,
                            renderAlias = measure.value.window != null
                        )
                    }

                    INTERMEDIATE_MERGE -> quoteIdentifier(measureName)
                }
            }

            is Dataset.Measure.MeasureValue.Dimension -> {
                val sqlMeasure = Dataset.Measure.MeasureValue.Sql("{{dimension.${measure.value.dimension}}}", measure.value.aggregation)

                when (queryType) {
                    ADHOC, INTERMEDIATE_ACCUMULATE -> context.renderSQL(
                        sqlMeasure.sql,
                        alias,
                        modelMeasure.datasetName
                    )

                    INTERMEDIATE_MERGE -> quoteIdentifier(measureName)
                }
            }
        }

        val baseJoinRelations = if (modelRelation != null) {
            generateJoinStatement(context, modelRelation)
        } else null

        // A filter might use a dimension from a relation. New joins may be added.
        // If the aggregation is not set, the only use case is it's a SQL
        val (value, joinRelations) = if (filters.isNotEmpty() && !isFilterInPushdown) {
            if (contextDatasetName == null) {
                throw MetriqlException(
                    "Measure '${measure.name}' can't be rendered in this context while it uses relational dimensions in filters and context model name is not set.",
                    HttpResponseStatus.BAD_REQUEST
                )
            }
            val renderedFilters = filters.map {
                renderFilter(
                    it,
                    contextDatasetName,
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
                val primaryKeyDimension = context.getModel(contextDatasetName)?.dimensions.find { it.primary == true }?.name
                    ?: throw MetriqlException("Primary key dimension is required for non-symmetric aggregates in `$contextDatasetName`", HttpResponseStatus.BAD_REQUEST)

                val primaryKey = renderDimension(context, contextDatasetName, primaryKeyDimension, null, null, MetricPositionType.FILTER)

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
                    ${nonSymmetricAggregates.map { "`${it.value.relation.name}` relation from model `${it.value.sourceDatasetName}`" }}
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

        val measureAlias = context.getMeasureAlias(measure.name, relationName)

        val measureValueForPositionType = when (metricPositionType) {
            MetricPositionType.FILTER -> renderedValue
            MetricPositionType.PROJECTION -> "$renderedValue AS $measureAlias"
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
        datasetName: DatasetName,
        datasetTarget: Dataset.Target,
        dimensions: List<Dataset.Dimension>,
    ): String {
        val modelTargetSQL = context.getSQLReference(
            datasetTarget,
            context.getOrGenerateAlias(datasetName, null),
            datasetName,
            null,
            dimensions.map { it.name },
            DateRange(LocalDate.now().minusDays(14), LocalDate.now()) // Dimension discovery filter on last 14 days. Especially helpful for bigquery
        )

        val projectedDimensions = if (dimensions.isNotEmpty()) {
            dimensions
                .joinToString(", ") { dimension ->
                    val value = when (dimension.value) {
                        is Dataset.Dimension.DimensionValue.Column -> context.getSQLReference(
                            datasetTarget,
                            context.getOrGenerateAlias(datasetName, null),
                            datasetName,
                            dimension.value.column
                        )

                        is Dataset.Dimension.DimensionValue.Sql -> context.renderSQL(dimension.value.sql, context.getOrGenerateAlias(datasetName, null), datasetName)
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
        contextDatasetName: DatasetName,
        dimensionName: DimensionName,
        relationName: RelationName?,
        context: IQueryGeneratorContext,
    ): Pair<ModelDimension, ModelRelation?> {
        return if (relationName != null) {
            val modelRelation = context.getRelation(contextDatasetName, relationName)
            // Get the dimensions model-name from joins target model
            val modelDimension = context.getModelDimension(dimensionName, modelRelation.targetDatasetName)
            Pair(modelDimension, modelRelation)
        } else {
            val modelDimension = context.getModelDimension(dimensionName, contextDatasetName)
            Pair(modelDimension, null)
        }
    }

    override fun renderDimension(
        context: IQueryGeneratorContext,
        contextDatasetName: DatasetName,
        dimensionName: DimensionName,
        relationName: RelationName?,
        timeframe: ReportMetric.Timeframe?,
        metricPositionType: MetricPositionType,
        modelAlias: String?
    ): WarehouseMetriqlBridge.RenderedField {
        val (modelDimension, modelRelation) = generateModelDimension(contextDatasetName, dimensionName, relationName, context)

        val dimension = modelDimension.dimension
        val isWindow = dimension.value is Dataset.Dimension.DimensionValue.Sql && dimension.value.window == true

        val modelAlias = modelAlias ?: context.getOrGenerateAlias(contextDatasetName, relationName)
        val rawValue = when (dimension.value) {
            is Dataset.Dimension.DimensionValue.Column -> context.getSQLReference(modelDimension.target, modelAlias, modelDimension.datasetName, dimension.value.column)
            is Dataset.Dimension.DimensionValue.Sql -> context.renderSQL(
                dimension.value.sql,
                context.getOrGenerateAlias(contextDatasetName, relationName),
                datasetName = modelDimension.datasetName,
                renderAlias = dimension.value.window != null
            )
        }

        val value = metricRenderHook.dimensionBeforePostOperation(context, metricPositionType, dimension, timeframe, rawValue)

        val postProcessedDimension = if (timeframe != null) { // && modelDimension.dimension.fieldType != null
            val template = when (timeframe.type) {
                ReportMetric.Timeframe.Type.TIMESTAMP -> timeframes.timestampPostOperations[timeframe.value]
                ReportMetric.Timeframe.Type.DATE -> timeframes.datePostOperations[timeframe.value]
                ReportMetric.Timeframe.Type.TIME -> timeframes.timePostOperations[timeframe.value]
            } ?: throw MetriqlException("Post operation is not supported", HttpResponseStatus.BAD_REQUEST)
            metricRenderHook.dimensionAfterPostOperation(context, metricPositionType, dimension, timeframe, String.format(template, value))
        } else {
            value
        }

        val joinRelations = if (modelRelation != null) generateJoinStatement(context, modelRelation) else null

        val alias = context.getDimensionAlias(dimensionName, relationName, timeframe)
        val dimensionValue = when (metricPositionType) {
            MetricPositionType.FILTER -> postProcessedDimension
            MetricPositionType.PROJECTION -> "$postProcessedDimension AS ${quoteIdentifier(alias)}"
        }

        return WarehouseMetriqlBridge.RenderedField(dimensionValue, joinRelations, isWindow, alias)
    }

    override fun generateJoinStatement(
        context: IQueryGeneratorContext,
        modelRelation: ModelRelation,
    ): String {
        val relation = modelRelation.relation
        val joinType = when (relation.joinType) {
            Dataset.Relation.JoinType.LEFT_JOIN -> "LEFT JOIN"
            Dataset.Relation.JoinType.INNER_JOIN -> "INNER JOIN"
            Dataset.Relation.JoinType.RIGHT_JOIN -> "RIGHT JOIN"
            Dataset.Relation.JoinType.FULL_JOIN -> "FULL JOIN"
        }
        val targetAlias = context.getOrGenerateAlias(modelRelation.sourceDatasetName, modelRelation.relation.name)
        val sourceAlias = context.getOrGenerateAlias(modelRelation.sourceDatasetName, null)

        val joinModel = context.getSQLReference(modelRelation.targetDatasetTarget, targetAlias, modelRelation.targetDatasetName, null)
        val joinExpression = when (relation.value) {
            is Dataset.Relation.RelationValue.SqlValue -> {
                context.renderSQL(
                    relation.value.sql,
                    targetAlias,
                    modelRelation.targetDatasetName,
                    extraContext = mapOf("TARGET" to context.getOrGenerateAlias(modelRelation.sourceDatasetName, modelRelation.relation.name))
                )
            }

            is Dataset.Relation.RelationValue.ColumnValue -> {
                val columnTypeRelation = relation.value
                "${
                context.getSQLReference(
                    modelRelation.sourceDatasetTarget,
                    sourceAlias,
                    modelRelation.sourceDatasetName,
                    columnTypeRelation.sourceColumn
                )
                } = ${
                context.getSQLReference(
                    modelRelation.targetDatasetTarget,
                    targetAlias,
                    modelRelation.targetDatasetName,
                    columnTypeRelation.targetColumn
                )
                }"
            }

            is Dataset.Relation.RelationValue.DimensionValue -> {
                "${
                renderDimension(
                    context,
                    modelRelation.sourceDatasetName,
                    relation.value.sourceDimension,
                    null,
                    null,
                    MetricPositionType.FILTER
                ).value
                } = ${
                renderDimension(
                    context,
                    modelRelation.targetDatasetName,
                    relation.value.targetDimension,
                    null,
                    null,
                    MetricPositionType.FILTER,
                    modelAlias = targetAlias
                ).value
                }"
            }
        }
        return "$joinType $joinModel ON ($joinExpression)"
    }

    // Append viewModels as WITH model AS (), model_other as ()
    override fun generateQuery(viewModels: Map<DatasetName, String>, rawQuery: String, comments: List<String>): String {
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
