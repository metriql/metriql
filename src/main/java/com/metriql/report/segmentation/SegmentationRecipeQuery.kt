package com.metriql.report.segmentation

import com.fasterxml.jackson.annotation.JsonAlias
import com.metriql.dbt.DbtJinjaRenderer
import com.metriql.report.data.recipe.OrFilters
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationReportOptions.Order.Type.DIMENSION
import com.metriql.report.segmentation.SegmentationReportOptions.Order.Type.MEASURE
import com.metriql.service.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.MaterializeQuery
import com.metriql.warehouse.spi.services.RecipeQuery
import io.netty.handler.codec.http.HttpResponseStatus

// SELECT `dimension`, `measure` FROM `dataset` GROUP BY `dimension` WHERE `filters` ORDER BY `orders` LIMIT `limit`
data class SegmentationRecipeQuery(
    @JsonAlias("model")
    val dataset: ModelName,
    val measures: List<Recipe.FieldReference>?,
    val dimensions: List<Recipe.FieldReference>?,
    val filters: List<OrFilters>?,
    val reportOptions: SegmentationReportOptions.ReportOptions? = null,
    val limit: Int? = null,
    val orders: Map<Recipe.FieldReference, Recipe.OrderType>? = null
) : RecipeQuery {
    override fun toReportOptions(context: IQueryGeneratorContext): SegmentationReportOptions {
        val regexModelName = DbtJinjaRenderer.renderer.renderModelNameRegex(dataset)
        val model = context.getModel(regexModelName)
        return SegmentationReportOptions(
            model.name,
            dimensions?.map { it.toDimension(model.name, it.getType(context, model.name)) },
            measures?.map { it.toMeasure(model.name) } ?: listOf(),
            filters?.map { it.toReportFilter(context, model.name) },
            reportOptions,
            limit = limit,
            orders = orders?.entries?.map { order ->
                val fieldModel = if (order.key.relation != null) {
                    model.relations.find { it.name == order.key.relation }!!.modelName
                } else model.name
                val targetModel = context.getModel(fieldModel)
                when {
                    targetModel.dimensions.any { it.name == order.key.name } -> {
                        SegmentationReportOptions.Order(DIMENSION, order.key.toDimension(model.name, order.key.getType(context, model.name)), order.value == Recipe.OrderType.ASC)
                    }
                    targetModel.measures.any { it.name == order.key.name } -> {
                        SegmentationReportOptions.Order(MEASURE, order.key.toMeasure(model.name), order.value == Recipe.OrderType.ASC)
                    }
                    else -> {
                        throw MetriqlException("Ordering field ${order.key} not found in $fieldModel", HttpResponseStatus.BAD_REQUEST)
                    }
                }
            }
        )
    }

    override fun toMaterialize(): SegmentationMaterialize {
        return SegmentationMaterialize(measures ?: listOf(), dimensions, filters)
    }

    data class SegmentationMaterialize(
        val measures: List<Recipe.FieldReference>,
        val dimensions: List<Recipe.FieldReference>?,
        val filters: List<OrFilters>?,
        val tableName : String? = null
    ) : MaterializeQuery {
        override fun toQuery(modelName: ModelName): RecipeQuery {
            return SegmentationRecipeQuery(modelName, measures, dimensions, filters)
        }
    }
}
