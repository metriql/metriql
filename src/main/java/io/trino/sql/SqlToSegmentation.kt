package io.trino.sql

import com.google.inject.Inject
import com.metriql.db.FieldType
import com.metriql.report.Recipe
import com.metriql.report.ReportFilter
import com.metriql.report.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.ReportFilter.FilterValue.MetricFilter.MetricType
import com.metriql.report.ReportFilter.Type.METRIC_FILTER
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.model.IModelService
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.warehouse.presto.PrestoMetriqlBridge
import com.metriql.warehouse.presto.PrestoWarehouse
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.TOTAL_ROWS_MEASURE
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND
import io.trino.sql.MetriqlExpressionFormatter.formatIdentifier
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import io.trino.sql.tree.AliasedRelation
import io.trino.sql.tree.AllColumns
import io.trino.sql.tree.BooleanLiteral
import io.trino.sql.tree.ComparisonExpression
import io.trino.sql.tree.DereferenceExpression
import io.trino.sql.tree.DoubleLiteral
import io.trino.sql.tree.Expression
import io.trino.sql.tree.FunctionCall
import io.trino.sql.tree.Identifier
import io.trino.sql.tree.Limit
import io.trino.sql.tree.Literal
import io.trino.sql.tree.LogicalBinaryExpression
import io.trino.sql.tree.LongLiteral
import io.trino.sql.tree.Node
import io.trino.sql.tree.QuerySpecification
import io.trino.sql.tree.Relation
import io.trino.sql.tree.SelectItem
import io.trino.sql.tree.SimpleGroupBy
import io.trino.sql.tree.SingleColumn
import io.trino.sql.tree.SortItem
import io.trino.sql.tree.StringLiteral
import io.trino.sql.tree.Table

class SqlToSegmentation @Inject constructor(val segmentationService: SegmentationService, val modelService: IModelService) {
    val parser = SqlParser()

    fun convert(context: IQueryGeneratorContext, query: QuerySpecification): String {
        if (query.offset.isPresent) {
            throw MetriqlException("Offset is not supported", HttpResponseStatus.BAD_REQUEST)
        }
        if (query.windows.isNotEmpty()) {
            throw MetriqlException("WINDOW operations not supported", HttpResponseStatus.BAD_REQUEST)
        }

        val models = modelService.list(context.auth)

        val (model, alias) = getModel(models, query.from.orElse(null))

        val references = mutableMapOf<Node, Pair<MetricType, String>>()
        buildReferences(references, model, context, alias, relation = null)

        model.relations.forEach { relation ->
            val relationModel = models.find { it.name == relation.modelName } ?: throw IllegalStateException()
            buildReferences(references, relationModel, context, alias, relation = relation)
        }

        val measures = mutableListOf<String>()
        val dimensions = mutableListOf<String>()

        // TODO: see how we can use it
        val groupings = query.groupBy.orElse(null)?.groupingElements?.flatMap {
            if (it !is SimpleGroupBy) {
                throw UnsupportedOperationException()
            }
            it.expressions.map { exp -> getReference(query.select.selectItems, exp) }
        }?.toList() ?: listOf()

        val projectionColumns = query.select.selectItems.map {
            when (it) {
                is AllColumns -> throw TODO()
                is SingleColumn -> {
                    val alias = it.alias.orElse(null)?.let { identifier -> identifier.value }
                        ?: deferenceExpression(it.expression, model.name)?.let { exp -> getIdentifierValue(exp) }
                    val projection = pushdownExpression(references, context, models, it.expression, dimensions, measures)
                    Pair(projection, alias)
                }
                else -> TODO()
            }
        }

        val whereFilters = query.where.orElse(null)?.let { processWhereExpression(references, model, it, models) } ?: listOf()
        val havingFilters = query.having.orElse(null)?.let { processWhereExpression(references, model, it, models) } ?: listOf()

        val query = SegmentationRecipeQuery(
            model.name,
            measures.map { Recipe.MetricReference.fromName(it) },
            dimensions.map { Recipe.DimensionReference.fromName(it) },
            (whereFilters + havingFilters).mapNotNull { it.toReference() },
            limit = parseLimit(query.limit.orElse(null)),
            orders = parseOrders(references, query)
        ).toReportOptions(context)

        val (renderedQuery, _, _) = segmentationService.renderQuery(context.auth, context, query)

        return if (projectionColumns.any { it.first != it.second }) {
            val quotedAlias = context.warehouseBridge.quoteIdentifier(alias)
            """SELECT ${
            projectionColumns.joinToString(", ") { col ->
                val alias = col.second?.let { context.warehouseBridge.quoteIdentifier(it) }
                if (col.first != col.second) "${col.first}${alias?.let { " AS $it" } ?: ""}" else col.first
            }
            } FROM (
                |$renderedQuery
                |) AS $quotedAlias""".trimMargin()
        } else {
            renderedQuery
        }
    }

    private fun deferenceExpression(expression: Expression, modelName: ModelName): Node? {
        return when (expression) {
            is DereferenceExpression -> {
                val source = getIdentifierValue(expression.base)
                if (source != modelName) {
                    throw IllegalArgumentException("Invalid reference $source")
                }
                expression.field
            }
            is Identifier -> expression
            is FunctionCall -> Identifier(expression.name.suffix)
            else -> null
        }
    }

    private fun getIdentifierValue(expression: Node): String {
        return when (expression) {
            is Identifier -> {
                expression.value
            }
            else -> TODO()
        }
    }

    private fun pushdownExpression(
        references: Map<Node, Pair<MetricType, String>>,
        context: IQueryGeneratorContext,
        models: List<Model>,
        expression: Expression,
        dimensions: MutableList<String>,
        measures: MutableList<String>
    ): String {
        return MetriqlQueryRewriter(context, models, references, dimensions, measures).process(expression, null)
    }

    inner class MetriqlQueryRewriter(
        context: IQueryGeneratorContext,
        models: List<Model>,
        private val references: Map<Node, Pair<MetricType, String>>,
        val dimensions: MutableList<String>,
        val measures: MutableList<String>,
    ) : MetriqlExpressionFormatter.Formatter(this, context, models) {
        override fun visitFunctionCall(node: FunctionCall, context: Void?): String? {
            if (node.name.prefix.isPresent) {
                throw UnsupportedOperationException("schema functions are not supported")
            }

            var directReference = references[node]
            val reference = if (directReference != null) {
                measures.add(directReference.second)
                directReference.second
            } else if (isPlain(node)) {
                // workaround for basic count(*) as we have a system measure for it
                val isTotalRows = when (node.name.suffix) {
                    "count" -> {
                        when {
                            node.arguments.isEmpty() -> true
                            node.arguments[0] is Literal -> true
                            else -> false
                        }
                    }
                    "sum" -> {
                        when (val arg = node.arguments[0]) {
                            is DoubleLiteral -> arg.value == 1.0
                            is LongLiteral -> arg.value == 1L
                            else -> false
                        }
                    }
                    else -> false
                }

                if (isTotalRows) {
                    measures.add(TOTAL_ROWS_MEASURE.name)
                    TOTAL_ROWS_MEASURE.name
                } else null
            } else null

            return if (reference != null) {
                formatIdentifier(reference, queryContext)
            } else {
                super.visitFunctionCall(node, context)
            }
        }

        private fun isPlain(node: FunctionCall): Boolean {
            return !node.isDistinct && node.filter.isEmpty &&
                node.nullTreatment.isEmpty && node.window.isEmpty && node.processingMode.isEmpty
        }

        override fun visitDereferenceExpression(node: DereferenceExpression, context: Void?): String? {
            val reference = references[node]
            return if (reference != null) {
                dimensions.add(reference.second)
                queryContext.warehouseBridge.quoteIdentifier(node.field.value)
            } else {
                super.visitDereferenceExpression(node, context)
            }
        }

        override fun visitIdentifier(node: Identifier, context: Void?): String {
            val reference = references[node]
            return if (reference != null) {
                dimensions.add(reference.second)
                node.value
            } else {
                super.visitIdentifier(node, context)
            }
        }
    }

    private fun getReference(selectItems: List<SelectItem>, exp: Expression): Expression {
        return when (exp) {
            is LongLiteral -> {
                val index = exp.value.toInt() - 1
                if (selectItems.size <= index) {
                    throw MetriqlException("Unable to parse GROUP BY ${exp.value}", HttpResponseStatus.BAD_REQUEST)
                }
                when (val selectItem = selectItems[index]) {
                    is SingleColumn -> selectItem.expression
                    is AllColumns -> throw TODO()
                    else -> throw IllegalStateException()
                }
            }
            is Identifier -> {
                val singleSelectItem = selectItems.find {
                    when (it) {
                        is SingleColumn -> it.alias.orElse(null) == exp
                        else -> false
                    }
                }

                (singleSelectItem as? SingleColumn)?.expression ?: exp
            }
            else -> exp
        }
    }

    private fun getModel(models: List<Model>, from: Relation?): Pair<Model, String> {
        if (from == null) {
            throw java.lang.UnsupportedOperationException()
        }

        return when (from) {
            is AliasedRelation -> {
                Pair(getModel(models, from.relation).first, from.alias.value)
            }
            is Table -> {
                val model = models.find { it.name == from.name.suffix } ?: throw MetriqlException("Model ${from.name.suffix} not found", NOT_FOUND)
                Pair(model, from.name.suffix)
            }
            else -> {
                TODO()
            }
        }
    }

    private fun parseOrders(references: Map<Node, Pair<MetricType, String>>, query: QuerySpecification): Map<Recipe.MetricReference, Recipe.OrderType>? {
        if (!query.orderBy.isPresent) {
            return null
        }

        return query.orderBy.get().sortItems.map {
            val orderType = when (it.ordering) {
                SortItem.Ordering.DESCENDING -> Recipe.OrderType.DESC
                SortItem.Ordering.ASCENDING -> Recipe.OrderType.ASC
            }
            if (it.nullOrdering != SortItem.NullOrdering.UNDEFINED) {
                throw MetriqlException("NULL ORDERING is not supported yet", HttpResponseStatus.BAD_REQUEST)
            }
            val reference = getReference(query.select.selectItems, it.sortKey)
            val metric = Recipe.MetricReference.fromName(references[reference]?.second ?: throw TODO())
            metric to orderType
        }.toMap()
    }

    private fun processWhereExpression(
        references: Map<Node, Pair<MetricType, String>>,
        model: Model,
        exp: Expression,
        models: List<Model>
    ): List<ReportFilter>? {
        return when (exp) {
            is LogicalBinaryExpression -> {
                val left = processWhereExpression(references, model, exp.left, models)
                val right = processWhereExpression(references, model, exp.right, models)
                val allFilters = (left ?: listOf()) + (right ?: listOf())

                when (exp.operator) {
                    LogicalBinaryExpression.Operator.AND -> allFilters
                    LogicalBinaryExpression.Operator.OR -> {
                        val filters = allFilters.flatMap { filter ->
                            when (filter.value) {
                                is ReportFilter.FilterValue.Sql -> throw UnsupportedOperationException()
                                is MetricFilter -> filter.value.filters
                            }
                        }
                        listOf(ReportFilter(METRIC_FILTER, MetricFilter(null, null, filters = filters)))
                    }
                }
            }
            is ComparisonExpression -> {
                val isRedundant = if (exp.left is Literal && exp.right is Literal) {
                    when (exp.operator) {
                        ComparisonExpression.Operator.EQUAL -> {
                            exp.left == exp.right
                        }
                        ComparisonExpression.Operator.NOT_EQUAL -> {
                            exp.left != exp.right
                        }
                        else -> TODO()
                    }
                } else null

                when (isRedundant) {
                    true -> listOf()
                    false -> null
                    else -> {
                        val metricReference = references[exp.left] ?: references[exp.right] ?: TODO()

                        val value = getFilterValue(if (references.containsKey(exp.left)) exp.right else exp.left)
                        val (type, metricValue) = when (metricReference.first) {
                            MetricType.DIMENSION -> {
                                val fromName = Recipe.DimensionReference.fromName(metricReference.second)
                                val type = fromName.getType({ models.first { m -> m.name == it } }, model.name)
                                type to fromName.toDimension(model.name, type)
                            }
                            MetricType.MEASURE -> FieldType.DOUBLE to Recipe.MetricReference.fromName(metricReference.second).toMeasure(model.name)
                            else -> throw IllegalStateException()
                        }
                        val operator = convertMetriqlOperator(exp.operator, type.operatorClass.java)

                        listOf(
                            ReportFilter(
                                METRIC_FILTER,
                                MetricFilter(
                                    metricReference.first, metricValue,
                                    listOf(MetricFilter.Filter(metricReference.first, metricValue, type, operator, value))
                                )
                            )
                        )
                    }
                }
            }
            is BooleanLiteral -> {
                if (exp.value) {
                    // redundant filter
                    null
                } else {
                    throw UnsupportedOperationException("`false` value is not supported in WHERE condition")
                }
            }
            else -> throw UnsupportedOperationException("${exp.javaClass.name} statement is not supported in WHERE condition")
        }
    }

    private fun convertMetriqlOperator(operator: ComparisonExpression.Operator, clazz: Class<out Enum<*>>): Enum<*> {
        return when (operator) {
            ComparisonExpression.Operator.EQUAL -> JsonHelper.convert("equals", clazz)
            ComparisonExpression.Operator.NOT_EQUAL -> JsonHelper.convert("notEquals", clazz)
            ComparisonExpression.Operator.LESS_THAN -> JsonHelper.convert("lessThan", clazz)
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL -> TODO()
            ComparisonExpression.Operator.GREATER_THAN -> JsonHelper.convert("greaterThan", clazz)
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL -> TODO()
            ComparisonExpression.Operator.IS_DISTINCT_FROM -> TODO()
        }
    }

    private fun parseLimit(limit: Node?): Int? {
        return when (limit) {
            null -> null
            is Limit -> {
                when (val count = limit.rowCount) {
                    is LongLiteral -> count.value.toInt()
                    else -> TODO()
                }
            }
            else -> TODO()
        }
    }

    private fun getFilterValue(exp: Expression): Any {
        return when (exp) {
            is LongLiteral -> exp.value
            is DoubleLiteral -> exp.value
            is StringLiteral -> exp.value
            else -> throw TODO()
        }
    }

    private fun getMeasureExp(context: IQueryGeneratorContext, model: Model, measure: Model.Measure, tableAlias: String?, prefix: String?): Expression {
        val exp = when (val value = measure.value) {
            is Model.Measure.MeasureValue.Column -> {

                val columnValue = value.column?.let {
                    val suffix = "${prefix ?: ""}${value.column}"
                    if (tableAlias != null) "$tableAlias.$suffix" else suffix
                } ?: ""
                // we support Presto warehouse as the Sql dialect so we can only support Presto aggregations
                PrestoWarehouse.bridge.performAggregation(columnValue, value.aggregation, WarehouseMetriqlBridge.AggregationContext.ADHOC)
            }
            is Model.Measure.MeasureValue.Sql -> context.renderSQL(value.sql, model.name)
            is Model.Measure.MeasureValue.Dimension -> {
                val dim = model.dimensions.find { d -> d.name == value.dimension } ?: throw TODO()
                PrestoWarehouse.bridge.performAggregation(
                    getDimensionStr(dim, tableAlias, prefix),
                    value.aggregation,
                    WarehouseMetriqlBridge.AggregationContext.ADHOC
                )
            }
        }
        return parseExpression(exp)
    }

    private fun getDimensionStr(dimension: Model.Dimension, tableAlias: String?, prefix: String?): String {
        // the mapping is based on presto dialect
        var suffix = PrestoMetriqlBridge.quoteIdentifier("${prefix?.let { "$it." } ?: ""}${dimension.name}")
        return if (tableAlias == null) suffix else (PrestoMetriqlBridge.quoteIdentifier(tableAlias) + "." + suffix)
    }

    private fun getDimensionExp(dimension: Model.Dimension, tableAlias: String?, prefix: String? = null): Expression {
        return parseExpression(getDimensionStr(dimension, tableAlias, prefix))
    }

    private fun buildReferences(
        references: MutableMap<Node, Pair<MetricType, String>>,
        sourceModel: Model,
        context: IQueryGeneratorContext,
        tableAlias: String,
        relation: Model.Relation? = null
    ) {
        val prefix = relation?.name?.let { "$it." } ?: ""

        sourceModel.dimensions.forEach {
            references[getDimensionExp(it, tableAlias, relation?.name)] = Pair(MetricType.DIMENSION, prefix + it.name)

            if (relation == null) {
                references[getDimensionExp(it, null, null)] = Pair(MetricType.DIMENSION, prefix + it.name)
            }
        }

        sourceModel.measures.forEach {
            references[getMeasureExp(context, sourceModel, it, tableAlias, relation?.name)] = Pair(MetricType.MEASURE, prefix + it.name)

            if (relation == null) {
                references[getMeasureExp(context, sourceModel, it, null, null)] = Pair(MetricType.MEASURE, prefix + it.name)
            }
        }
    }

    private fun parseExpression(expression: String): Expression {
        return parser.createExpression(expression, ParsingOptions())
    }
}
