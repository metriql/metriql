package io.trino.sql

import com.google.inject.Inject
import com.hubspot.jinjava.Jinjava
import com.metriql.db.FieldType
import com.metriql.report.Recipe
import com.metriql.report.ReportFilter
import com.metriql.report.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.ReportFilter.FilterValue.MetricFilter.MetricType
import com.metriql.report.ReportFilter.Type.METRIC_FILTER
import com.metriql.report.ReportMetric
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.model.IModelService
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import io.trino.sql.tree.AliasedRelation
import io.trino.sql.tree.AllColumns
import io.trino.sql.tree.ComparisonExpression
import io.trino.sql.tree.DefaultTraversalVisitor
import io.trino.sql.tree.DereferenceExpression
import io.trino.sql.tree.DoubleLiteral
import io.trino.sql.tree.Expression
import io.trino.sql.tree.FunctionCall
import io.trino.sql.tree.Identifier
import io.trino.sql.tree.Limit
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
    private lateinit var groupings: List<Node>
    private lateinit var model: Model
    private lateinit var references: Map<Node, String>
    private lateinit var projectionColumns: List<Pair<String, String>>
    val renderer = Jinjava()

    fun convert(context: IQueryGeneratorContext, query: QuerySpecification): String {
        if (query.offset.isPresent) {
            throw MetriqlException("Offset is not supported", HttpResponseStatus.BAD_REQUEST)
        }
        if (query.windows.isNotEmpty()) {
            throw MetriqlException("WINDOW operations not supported", HttpResponseStatus.BAD_REQUEST)
        }

        val models = modelService.list(context.auth)

        val (model, alias) = getModel(models, query.from.orElse(null))

        this.model = model

        references = buildReferences(model, context, alias, metricPrefix = null)

        val measures = mutableListOf<String>()
        val dimensions = mutableListOf<String>()

        this.groupings = query.groupBy.orElse(null)?.groupingElements?.flatMap {
            if (it !is SimpleGroupBy) {
                throw UnsupportedOperationException()
            }
            it.expressions.map { exp -> getReference(query.select.selectItems, exp) }
        }?.toList() ?: listOf()

        for (selectItem in query.select.selectItems) {
            when (selectItem) {
                is AllColumns -> throw TODO()
                is SingleColumn -> {

                    val metricReferences = pushdownExpression(selectItem.expression)

                    metricReferences.forEach { metricReference ->
                        val isDimension = model.dimensions.any { it.name == metricReference }

                        if (isDimension) {
                            dimensions.add(metricReference)
                        } else {
                            measures.add(metricReference)
                        }
                    }
                }
            }
        }

        projectionColumns = query.select.selectItems.map {
            when (it) {
                is AllColumns -> throw TODO()
                is SingleColumn -> {
                    val alias = it.alias.orElse(null)?.let { identifier -> identifier.value } ?: getIdentifierValue(deferenceExpression(it.expression, model.name))
                    val projection = MetriqlExpressionFormatter.formatExpression(it.expression, this, context, models)
                    Pair(projection, alias)
                }
                else -> TODO()
            }
        }

        val whereFilters = query.where.orElse(null)?.let { processWhereExpression(it, models) } ?: listOf()
        val havingFilters = query.having.orElse(null)?.let { processWhereExpression(it, models) } ?: listOf()

        val query = SegmentationRecipeQuery(
            model.name,
            measures.map { Recipe.MetricReference.fromName(it) },
            dimensions.map { Recipe.DimensionReference.fromName(it) },
            (whereFilters + havingFilters).mapNotNull { it.toReference() },
            limit = parseLimit(query.limit.orElse(null)),
            orders = parseOrders(query)
        ).toReportOptions(context)

        val (renderedQuery, _, _) = segmentationService.renderQuery(context.auth, context, query)

        return if (projectionColumns.any { it != null }) {
            """SELECT ${projectionColumns.map { col -> col?.let { "${it.first} AS ${it.second}" } }.joinToString(",")} FROM ($renderedQuery) ${
                ValidationUtil.quoteIdentifier(
                    alias,
                    context.getAliasQuote()
                )
            }"""
        } else {
            renderedQuery
        }
    }

    private fun deferenceExpression(expression: Expression?, modelName: ModelName): Node {
        return when (expression) {
            is DereferenceExpression -> {
                val source = getIdentifierValue(expression.base)
                if (source != modelName) {
                    throw IllegalArgumentException("Invalid reference $source")
                }
                expression.field
            }
            else -> TODO()
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

    private fun pushdownExpression(expression: Expression): List<String> {
        val references = mutableListOf<String>()
        YarrakVisitor().process(expression, references)
        return references
    }

    class YarrakVisitor : DefaultTraversalVisitor<List<String>>() {
        override fun visitFunctionCall(node: FunctionCall?, context: List<String>?): Void {
            return super.visitFunctionCall(node, context)
        }

        override fun visitDereferenceExpression(node: DereferenceExpression?, context: List<String>?): Void {
            return super.visitDereferenceExpression(node, context)
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
                val model = models.find { it.name == from.name.suffix } ?: throw MetriqlException("Model ${from.name.suffix} not found", HttpResponseStatus.NOT_FOUND)
                Pair(model, from.name.suffix)
            }
            else -> {
                TODO()
            }
        }
    }

    private fun parseOrders(query: QuerySpecification): Map<Recipe.MetricReference, Recipe.OrderType>? {
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
            val metric = Recipe.MetricReference.fromName(references[getReference(query.select.selectItems, it.sortKey)] ?: throw TODO())
            metric to orderType
        }.toMap()
    }

    private fun processWhereExpression(exp: Expression, models: List<Model>): List<ReportFilter> {
        return when (exp) {
            is LogicalBinaryExpression -> {
                val left = processWhereExpression(exp.left, models)
                val right = processWhereExpression(exp.right, models)

                // TODO
                val metricType: MetricType = null!!
                val metricValue: ReportMetric = null!!

                when (exp.operator) {
                    LogicalBinaryExpression.Operator.AND -> {
                        listOf(
                            ReportFilter(METRIC_FILTER, MetricFilter(metricType, metricValue, filters = listOf())),
                            ReportFilter(METRIC_FILTER, MetricFilter(metricType, metricValue, filters = listOf())),
                        )
                    }
                    LogicalBinaryExpression.Operator.OR -> {
                        listOf(ReportFilter(METRIC_FILTER, MetricFilter(metricType, metricValue, filters = listOf(null!!, null!!))))
                    }
                }
            }
            is ComparisonExpression -> {
                val metricReference = references[exp.left] ?: references[exp.right] ?: throw TODO()

                val value = getFilterValue(if (references.containsKey(exp.left)) exp.right else exp.left)
                val metricType = if (groupings.contains(exp.left) || groupings.contains(exp.right)) MetricType.DIMENSION else MetricType.MEASURE
                val (type, metricValue) = when (metricType) {
                    MetricType.DIMENSION -> {
                        val fromName = Recipe.DimensionReference.fromName(metricReference)
                        val type = fromName.getType({ models.first { m -> m.name == it } }, model.name)
                        type to fromName.toDimension(model.name, type)
                    }
                    MetricType.MEASURE -> FieldType.DOUBLE to Recipe.MetricReference.fromName(metricReference).toMeasure(model.name)
                    else -> TODO()
                }
                val operator = when (exp.operator) {
                    ComparisonExpression.Operator.EQUAL -> {
                        JsonHelper.convert("equals", type.operatorClass.java)
                    }
                    ComparisonExpression.Operator.NOT_EQUAL -> {
                        JsonHelper.convert("notEquals", type.operatorClass.java)
                    }
                    else -> throw TODO()
                }

                listOf(ReportFilter(METRIC_FILTER, MetricFilter(metricType, metricValue, listOf(MetricFilter.Filter(type, operator, value)))))
            }
            else -> throw TODO()
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

    private fun getDimensionExp(context: IQueryGeneratorContext, name: String, alias: String): String {
        val aliasQuote = context.datasource.warehouse.bridge.aliasQuote
        return "$aliasQuote$alias$aliasQuote.$aliasQuote$name$aliasQuote"
    }

    private fun buildReferences(model: Model, context: IQueryGeneratorContext, tableAlias: String, metricPrefix: String? = null): Map<Node, String> {
        val references = mutableMapOf<Node, String>()
        val prefix = metricPrefix?.let { "$it." } ?: ""
        model.dimensions.forEach {
            val dimensionExp = getDimensionExp(context, it.name, tableAlias)
            references[parseExpression(dimensionExp)] = prefix + it.name
        }

        model.measures.forEach {
            val measureExp = when (val value = it.value) {
                is Model.Measure.MeasureValue.Column -> {
                    val columnValue = value.column?.let { "$tableAlias.${value.column}" } ?: ""
                    context.datasource.warehouse.bridge.performAggregation(columnValue, value.aggregation, WarehouseMetriqlBridge.AggregationContext.ADHOC)
                }
                is Model.Measure.MeasureValue.Sql -> context.renderSQL(value.sql, model.name)
                is Model.Measure.MeasureValue.Dimension -> {
                    val dim = model.dimensions.find { d -> d.name == value.dimension } ?: throw TODO()
                    context.datasource.warehouse.bridge.performAggregation(
                        getDimensionExp(context, dim.name, tableAlias),
                        value.aggregation,
                        WarehouseMetriqlBridge.AggregationContext.ADHOC
                    )
                }
            }

            references[parseExpression(measureExp)] = prefix + it.name
        }

        return references
    }

    fun render(template: String, alias: String): String {
        return renderer.render(template, mapOf("this" to alias))
    }

    private fun parseExpression(expression: String): Expression {
        return SqlParser().createExpression(expression, ParsingOptions())
    }
}
