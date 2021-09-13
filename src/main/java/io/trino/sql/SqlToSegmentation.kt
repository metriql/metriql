package io.trino.sql

import com.google.inject.Inject
import com.metriql.db.FieldType
import com.metriql.dbt.DbtJinjaRenderer
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.data.ReportFilter.FilterValue.MetricFilter.MetricType
import com.metriql.report.data.ReportFilter.Type.METRIC_FILTER
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.jdbc.StatementService.Companion.defaultParsingOptions
import com.metriql.service.model.IModelService
import com.metriql.service.model.Model
import com.metriql.service.model.Model.Measure.AggregationType.APPROXIMATE_UNIQUE
import com.metriql.service.model.Model.Measure.AggregationType.COUNT
import com.metriql.service.model.Model.Measure.AggregationType.COUNT_UNIQUE
import com.metriql.service.model.ModelName
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.presto.PrestoMetriqlBridge.quoteIdentifier
import com.metriql.warehouse.presto.PrestoWarehouse
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.ADHOC
import com.metriql.warehouse.spi.filter.AnyOperatorType
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.TOTAL_ROWS_MEASURE
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.sql.MetriqlExpressionFormatter.formatIdentifier
import io.trino.sql.parser.SqlParser
import io.trino.sql.tree.AllColumns
import io.trino.sql.tree.BetweenPredicate
import io.trino.sql.tree.BooleanLiteral
import io.trino.sql.tree.ComparisonExpression
import io.trino.sql.tree.DereferenceExpression
import io.trino.sql.tree.DoubleLiteral
import io.trino.sql.tree.Expression
import io.trino.sql.tree.FunctionCall
import io.trino.sql.tree.Identifier
import io.trino.sql.tree.InPredicate
import io.trino.sql.tree.IsNotNullPredicate
import io.trino.sql.tree.IsNullPredicate
import io.trino.sql.tree.LikePredicate
import io.trino.sql.tree.Limit
import io.trino.sql.tree.Literal
import io.trino.sql.tree.LogicalBinaryExpression
import io.trino.sql.tree.LongLiteral
import io.trino.sql.tree.Node
import io.trino.sql.tree.OrderBy
import io.trino.sql.tree.Relation
import io.trino.sql.tree.SelectItem
import io.trino.sql.tree.SingleColumn
import io.trino.sql.tree.SortItem
import io.trino.sql.tree.StringLiteral
import io.trino.sql.tree.Table
import java.util.Optional

typealias Reference = Pair<MetricType, String>

class SqlToSegmentation @Inject constructor(val segmentationService: SegmentationService, val modelService: IModelService) {
    val parser = SqlParser()

    fun convert(
        context: IQueryGeneratorContext,
        modelAlias: Pair<Model, String>,
        selectItems: MutableList<out SelectItem>,
        where: Optional<Expression>,
        having: Optional<Expression>,
        limit: Optional<Node>,
        orderBy: Optional<OrderBy>
    ): String {
        val (model, alias) = modelAlias

        val references = mutableMapOf<Node, Reference>()
        model.relations.forEach { relation ->
            val relationModel = context.getModel(relation.modelName)
            buildReferences(references, relationModel, context, alias, relation = relation)
        }

        // override values if they already exist
        buildReferences(references, model, context, alias, relation = null)

        val measures = mutableListOf<String>()
        val dimensions = mutableListOf<String>()

        // TODO: see how we can use it
//        val groupings = query.groupBy.orElse(null)?.groupingElements?.flatMap {
//            if (it !is SimpleGroupBy) {
//                throw UnsupportedOperationException()
//            }
//            it.expressions.map { exp -> getReference(query.select.selectItems, exp) }
//        }?.toList() ?: listOf()

        val projectionColumns = selectItems.map {
            when (it) {
                is AllColumns -> throw TODO()
                is SingleColumn -> {
                    val alias = it.alias.orElse(null)?.let { identifier -> identifier.value }
                        ?: deferenceExpression(it.expression, model.name)?.let { exp -> getIdentifierValue(exp) }
                    val projection = MetriqlSegmentationQueryRewriter(context, model, references, dimensions, measures).process(it.expression, null)
                    Pair(projection, alias)
                }
                else -> throw IllegalStateException()
            }
        }

        val whereFilters = where.orElse(null)?.let { processWhereExpression(context, references, model, it) } ?: listOf()
        val havingFilters = having.orElse(null)?.let { processWhereExpression(context, references, model, it) } ?: listOf()

        val query = SegmentationRecipeQuery(
            model.name,
            measures.map { Recipe.MetricReference.fromName(it) },
            dimensions.map { Recipe.DimensionReference.fromName(it) },
            (whereFilters + havingFilters).mapNotNull { it.toReference() },
            limit = parseLimit(limit.orElse(null)),
            orders = parseOrders(references, selectItems, orderBy)
        ).toReportOptions(context)

        val (renderedQuery, _, _) = segmentationService.renderQuery(context.auth, context, query)

        return if (projectionColumns.any { it.first != it.second && it.second != null }) {
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

    private fun deferenceExpression(expression: Expression, modelName: ModelName?): Node? {
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

    inner class MetriqlSegmentationQueryRewriter(
        context: IQueryGeneratorContext,
        private val model: Model,
        private val references: Map<Node, Reference>,
        val dimensions: MutableList<String>,
        val measures: MutableList<String>,
    ) : MetriqlExpressionFormatter.Formatter(this, context) {
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
                rewriteValueForReference(reference, node.field.value)
            } else {
                super.visitDereferenceExpression(node, context)
            }
        }

        override fun visitIdentifier(node: Identifier, context: Void?): String {
            val reference = references[node]
            return if (reference != null) {
                rewriteValueForReference(reference, node.value)
            } else {
                throw MetriqlException("Not found $node", HttpResponseStatus.BAD_REQUEST)
            }
        }

        private fun rewriteValueForReference(reference: Reference, value: String): String {
            return when (reference.first) {
                MetricType.DIMENSION -> {
                    dimensions.add(reference.second)
                    val ref = Recipe.DimensionReference.fromName(value)
                    val dimension = ref.toDimension(model.name, ref.getType(queryContext::getModel, model.name))
                    queryContext.getDimensionAlias(dimension.name, dimension.relationName, dimension.postOperation)
                }
                MetricType.MEASURE -> {
                    measures.add(reference.second)
                    val ref = Recipe.MetricReference.fromName(value)
                    queryContext.getMeasureAlias(ref.name, ref.relation)
                }
                else -> TODO()
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

    private fun parseOrders(references: Map<Node, Reference>, selectItems: List<SelectItem>, orderBy: Optional<OrderBy>): Map<Recipe.MetricReference, Recipe.OrderType>? {
        if (!orderBy.isPresent) {
            return null
        }

        return orderBy.get().sortItems.map {
            val orderType = when (it.ordering) {
                SortItem.Ordering.DESCENDING -> Recipe.OrderType.DESC
                SortItem.Ordering.ASCENDING -> Recipe.OrderType.ASC
            }
            if (it.nullOrdering != SortItem.NullOrdering.UNDEFINED) {
                throw MetriqlException("NULL ORDERING is not supported yet", HttpResponseStatus.BAD_REQUEST)
            }
            val reference = getReference(selectItems, it.sortKey)
            val metric = Recipe.MetricReference.fromName(references[reference]?.second ?: throw TODO())
            metric to orderType
        }.toMap()
    }

    private fun getReportFilter(
        context: IQueryGeneratorContext,
        model: Model,
        metricReference: Reference,
        operatorFunction: (FieldType) -> Enum<*>,
        value: Any?
    ): List<ReportFilter> {
        val (type, metricValue) = when (metricReference.first) {
            MetricType.DIMENSION -> {
                val fromName = Recipe.DimensionReference.fromName(metricReference.second)
                val type = fromName.getType({ context.getModel(it) }, model.name)
                type to fromName.toDimension(model.name, type)
            }
            MetricType.MEASURE -> FieldType.DOUBLE to Recipe.MetricReference.fromName(metricReference.second).toMeasure(model.name)
            else -> throw IllegalStateException()
        }

        return listOf(
            ReportFilter(
                METRIC_FILTER,
                MetricFilter(
                    metricReference.first, metricValue,
                    listOf(MetricFilter.Filter(metricReference.first, metricValue, type, operatorFunction.invoke(type), value))
                )
            )
        )
    }

    private fun extractReference(exp : Expression) {

    }

    private fun processWhereExpression(
        context: IQueryGeneratorContext,
        references: Map<Node, Reference>,
        model: Model,
        exp: Expression
//    ): Pair<List<ReportFilter>?, List<String>>  {
    ): List<ReportFilter>? {

        return when (exp) {
            is IsNullPredicate -> {
                val metricReference = references[exp.value] ?: TODO()
                getReportFilter(context, model, metricReference, { AnyOperatorType.IS_NOT_SET }, null)
            }
            is IsNotNullPredicate -> {
                val metricReference = references[exp.value] ?: TODO()
                getReportFilter(context, model, metricReference, { AnyOperatorType.IS_SET }, null)
            }
            is InPredicate -> {
                val metricReference = references[exp.value] ?: TODO()
                getReportFilter(
                    context, model, metricReference,
                    {
                        TODO()
                    },
                    null
                )
            }
            is BetweenPredicate -> {
                val metricReference = references[exp.value] ?: TODO()
                getReportFilter(
                    context, model, metricReference,
                    {
                        exp.min
                        TODO()
                    },
                    null
                )
            }
            is LikePredicate -> {
                val metricReference = references[exp.value] ?: TODO()
                getReportFilter(
                    context, model, metricReference,
                    {
                        exp.value
                        TODO()
                    },
                    null
                )
            }
            is LogicalBinaryExpression -> {
                val left = processWhereExpression(context, references, model, exp.left)
                val right = processWhereExpression(context, references, model, exp.right)
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
                        getReportFilter(context, model, metricReference, { convertMetriqlOperator(exp.operator, it.operatorClass.java) }, value)
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

    private fun getMeasureStr(measure: Model.Measure, tableAlias: String, prefix: String?): List<String> {
        val aggregations = when {
            measure.value.agg == APPROXIMATE_UNIQUE -> {
                listOf(APPROXIMATE_UNIQUE, COUNT_UNIQUE, COUNT, null)
            }
            measure.value.agg != null -> listOf(measure.value.agg, COUNT, null)
            else -> listOf(COUNT, null)
        }


        val suffix = ValidationUtil.quoteIdentifier("${prefix?.let { "$it." } ?: ""}${measure.name}")
        val columnValues = listOf(suffix, "$tableAlias.$suffix")

        return columnValues.flatMap {
            aggregations.map { aggregation ->
                if(aggregation != null) {
                    PrestoWarehouse.bridge.performAggregation(it, aggregation, ADHOC)
                } else {
                    it
                }
            }
        }
    }

    private fun getDimensionStr(dimension: Model.Dimension, tableAlias: String, prefix: String?): List<Pair<String, Reference>> {

        // the mapping is based on presto dialect
        val identifier = "${prefix?.let { "$it." } ?: ""}${dimension.name}"
        val baseReference = Pair(MetricType.DIMENSION, identifier)

        var quotedIdentifier = quoteIdentifier(identifier)
        val rawDimension = listOf(quotedIdentifier to baseReference, quoteIdentifier(tableAlias) + "." + quotedIdentifier to baseReference)
        return if (dimension.postOperations == null) {
            rawDimension
        } else {
            rawDimension + dimension.postOperations.flatMap {
                val identifier = "$identifier::$it"
                val quotedIdentifier = quoteIdentifier(identifier)
                val reference = Pair(MetricType.DIMENSION, identifier)

                listOf(
                    quotedIdentifier to reference,
                    quoteIdentifier(tableAlias) + "." + quotedIdentifier to reference
                )
            }
        }
    }

    private fun buildReferences(
        references: MutableMap<Node, Reference>,
        sourceModel: Model,
        context: IQueryGeneratorContext,
        tableAlias: String,
        relation: Model.Relation? = null
    ) {

        sourceModel.dimensions.forEach { dimension ->
            getDimensionStr(dimension, tableAlias, relation?.name).forEach { references[parseExpression(it.first)] = it.second }
        }

        sourceModel.measures.forEach { measure ->
            val prefix = relation?.name?.let { "$it." } ?: ""
            val reference = Pair(MetricType.MEASURE, prefix + measure.name)
            getMeasureStr(measure, tableAlias, relation?.name).forEach { references[parseExpression(it)] = reference }
        }
    }

    private fun parseExpression(expression: String): Expression {
        return parser.createExpression(expression, defaultParsingOptions)
    }

    companion object {
        fun getModel(context: IQueryGeneratorContext, from: Relation): Pair<Model, String> {
            return when (from) {
                is Table -> {
                    val table = DbtJinjaRenderer.renderer.renderModelNameRegex(from.name.suffix)
                    val model = context.getModel(table)
                    Pair(model, model.name)
                }
                else -> {
                    throw UnsupportedOperationException(String.format("Unsupported operation %s", from.javaClass.name))
                }
            }
        }
    }
}
