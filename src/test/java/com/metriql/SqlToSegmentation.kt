package com.metriql

import com.hubspot.jinjava.Jinjava
import com.metriql.db.FieldType
import com.metriql.report.Recipe
import com.metriql.report.Recipe.RecipeModel.Metric.RecipeDimension
import com.metriql.report.Recipe.RecipeModel.Metric.RecipeMeasure
import com.metriql.report.ReportFilter
import com.metriql.report.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.ReportFilter.FilterValue.MetricFilter.MetricType
import com.metriql.report.ReportFilter.Type.METRIC_FILTER
import com.metriql.report.ReportMetric
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.service.model.Model
import com.metriql.service.model.Model.Measure.AggregationType.SUM
import com.metriql.util.JsonHelper
import com.metriql.warehouse.snowflake.SnowflakeMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import net.sf.jsqlparser.expression.DoubleValue
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.parser.CCJSqlParserUtil.parseExpression
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.select.AllColumns
import net.sf.jsqlparser.statement.select.AllTableColumns
import net.sf.jsqlparser.statement.select.FromItem
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.SelectExpressionItem
import net.sf.jsqlparser.statement.select.WithItem

val bridge = SnowflakeMetriqlBridge
val renderer = Jinjava()

fun main() {

    val stmt = CCJSqlParserUtil.parse(
        """
        select tbl1.a, tbl2.a * 2, sum(tbl1.a) from tbl1 left join tbl2 on (tbl1.a = tbl2.a) 
        where tbl2.a * 2 = 100 group by 1,2  order by 3 desc
        """.trimIndent()
    )

    if (stmt !is Select) {
        throw IllegalArgumentException()
    }

    val models = listOf(
        Recipe.RecipeModel(
            "tbl1",
            false,
            null,
            "select 2 as a, 4 as b",
            null,
            measures = mapOf("sum_of_a" to RecipeMeasure(aggregation = SUM, dimension = "a")),
            dimensions = mapOf("a" to RecipeDimension(column = "a")),
            relations = mapOf("tbl2" to Recipe.RecipeModel.RecipeRelation(source = "a", target = "a", model = "tbl2"))
        ),
        Recipe.RecipeModel(
            "tbl2",
            false,
            null,
            "select 1 as a",
            null,
            dimensions = mapOf("a" to RecipeDimension(sql = "{{this}}.a * 2", type = FieldType.STRING))
        )
    ).map { it.toModel("", bridge, -1) }

    val selectBody = when (val selectBody = stmt.selectBody) {
        is WithItem -> {
            if (selectBody.isRecursive) {
                throw TODO()
            }

            if (selectBody.withItemList.isNotEmpty()) {
                throw TODO()
            }

            val body = selectBody.selectBody
            if (body !is PlainSelect) {
                throw TODO()
            }

            body
        }
        is PlainSelect -> selectBody
        else -> throw TODO()
    }

    val references = mutableMapOf<String, String>()

    val mainModel = getModel(models, selectBody.fromItem)
    addReferences(references, mainModel, getAlias(selectBody.fromItem), metricPrefix = null)

    selectBody.joins.forEach { join ->
        val model = getModel(models, join.rightItem)
        val tableAlias = getAlias(join.rightItem)
        val relation = mainModel.relations.find {
            it.modelName == model.name && when (it.joinType) {
                Model.Relation.JoinType.INNER_JOIN -> join.isInner
                Model.Relation.JoinType.LEFT_JOIN -> join.isLeft
                Model.Relation.JoinType.RIGHT_JOIN -> join.isRight
                Model.Relation.JoinType.FULL_JOIN -> join.isFull
            }
        }?.name ?: throw TODO()
        addReferences(references, model, tableAlias, metricPrefix = relation)
    }

    val measures = mutableListOf<String>()
    val dimensions = mutableListOf<String>()

    val groupings = selectBody.groupBy.groupByExpressions.map {
        when (it) {
            is LongValue -> selectBody.selectItems[it.value.toInt() - 1]
            else -> it
        }.toString()
    }.toList()

    for (selectItem in selectBody.selectItems) {
        when (selectItem) {
            is AllColumns, is AllTableColumns -> throw TODO()
            is SelectExpressionItem -> {
                val expressionText = selectItem.expression.toString()
                val isDimension = groupings.contains(expressionText)

                val normalizedExpression = normalizeExpressionForMetric(selectItem.expression)
                val metricReference = references[normalizedExpression] ?: throw TODO()

                if (isDimension) {
                    dimensions.add(metricReference)
                } else {
                    measures.add(metricReference)
                }
            }
        }
    }

    fun processWhereExpression(exp: Expression): List<ReportFilter> {
        return when (exp) {
            is AndExpression -> {
                processWhereExpression(exp.leftExpression)
                processWhereExpression(exp.rightExpression)

                // TODO
                val metricType: MetricType = null!!
                val metricValue: ReportMetric = null!!

                listOf(
                    ReportFilter(METRIC_FILTER, MetricFilter(metricType, metricValue, filters = listOf())),
                    ReportFilter(METRIC_FILTER, MetricFilter(metricType, metricValue, filters = listOf())),
                )
            }
            is OrExpression -> {
                processWhereExpression(exp.leftExpression)
                processWhereExpression(exp.rightExpression)

                // TODO
                val metricType: MetricType = null!!
                val metricValue: ReportMetric = null!!

                listOf(ReportFilter(METRIC_FILTER, MetricFilter(metricType, metricValue, filters = listOf(null!!, null!!))))
            }
            is ComparisonOperator -> {
                val leftExp = exp.leftExpression.toString()
                val rightExp = exp.rightExpression.toString()

                val metricReference = references[leftExp] ?: references[rightExp] ?: throw TODO()
                val value = getFilterValue(if (references.containsKey(leftExp)) exp.rightExpression else exp.leftExpression)
                val metricType = if (groupings.contains(leftExp) || groupings.contains(rightExp)) MetricType.DIMENSION else MetricType.MEASURE
                val (type, metricValue) = when (metricType) {
                    MetricType.DIMENSION -> {
                        val fromName = Recipe.DimensionReference.fromName(metricReference)
                        val type = fromName.getType({ models.first { m -> m.name == it } }, mainModel.name)
                        type to fromName.toDimension(mainModel.name, type)
                    }
                    MetricType.MEASURE -> FieldType.DOUBLE to Recipe.MetricReference.fromName(metricReference).toMeasure(mainModel.name)
                    else -> TODO()
                }
                val operator = when (exp.stringExpression) {
                    "=" -> {
                        JsonHelper.convert("equals", type.operatorClass.java)
                    }
                    "!=" -> {
                        JsonHelper.convert("notEquals", type.operatorClass.java)
                    }
                    else -> throw TODO()
                }

                listOf(ReportFilter(METRIC_FILTER, MetricFilter(metricType, metricValue, listOf(MetricFilter.Filter(type, operator, value)))))
            }
            else -> throw TODO()
        }
    }

    val filters = processWhereExpression(selectBody.where)

    val limit = if (selectBody.limit != null) {
        if (selectBody.limit.offset != null) {
            throw TODO()
        }

        val rowCount = selectBody.limit.rowCount
        if (rowCount !is LongValue) {
            throw TODO()
        }

        rowCount.value.toInt()
    } else null

    val query = SegmentationRecipeQuery(
        mainModel.name,
        measures.map { Recipe.MetricReference.fromName(it) },
        dimensions.map { Recipe.DimensionReference.fromName(it) },
        filters.mapNotNull { it.toReference() },
        limit = limit
    )

    // converted sql to segmentation query
    println(query)
}

fun normalizeExpressionForMetric(expression: Expression): String {
    return expression.toString()
}

fun getFilterValue(exp: Expression): Any {
    return when (exp) {
        is LongValue -> exp.value
        is DoubleValue -> exp.value
        is StringValue -> exp.value
        else -> throw TODO()
    }
}

fun getAlias(from: FromItem): String {
    return from.alias?.name ?: from.toString()
}

fun getDimensionExp(value: Model.Dimension.DimensionValue, alias: String): String {
    return when (value) {
        is Model.Dimension.DimensionValue.Column -> "$alias.${value.column}"
        is Model.Dimension.DimensionValue.Sql -> render(value.sql, alias)
    }
}

fun addReferences(sources: MutableMap<String, String>, model: Model, tableAlias: String, metricPrefix: String? = null) {
    val prefix = metricPrefix?.let { "$it." } ?: ""
    model.dimensions.forEach {
        val dimensionExp = getDimensionExp(it.value, tableAlias)
        sources[parseExpression(dimensionExp).toString()] = prefix + it.name
    }

    model.measures.forEach {
        val measureExp = when (val value = it.value) {
            is Model.Measure.MeasureValue.Column -> {
                val columnValue = value.column?.let { "$tableAlias.${value.column}" } ?: ""
                bridge.performAggregation(columnValue, value.aggregation, WarehouseMetriqlBridge.AggregationContext.ADHOC)
            }
            is Model.Measure.MeasureValue.Sql -> render(value.sql, tableAlias)
            is Model.Measure.MeasureValue.Dimension -> {
                val dim = model.dimensions.find { d -> d.name == value.dimension }?.value ?: throw TODO()
                bridge.performAggregation(getDimensionExp(dim, tableAlias), value.aggregation, WarehouseMetriqlBridge.AggregationContext.ADHOC)
            }
        }

        sources[parseExpression(measureExp).toString()] = prefix + it.name
    }
}

fun getModel(models: List<Model>, from: FromItem): Model {
    return when (from) {
        is Table -> {
            models.find { it.name == from.name }!!
        }
        else -> throw TODO()
    }
}

fun render(template: String, alias: String): String {
    return renderer.render(template, mapOf("this" to alias))
}
