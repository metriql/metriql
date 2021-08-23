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

data class SegmentationRecipeQuery(
    @JsonAlias("model")
    val dataset: ModelName,
    val measures: List<Recipe.MetricReference>?,
    val dimensions: List<Recipe.DimensionReference>?,
    val filters: List<OrFilters>?,
    val reportOptions: SegmentationReportOptions.ReportOptions? = null,
    val limit: Int? = null,
    val orders: Map<Recipe.MetricReference, Recipe.OrderType>? = null
) : RecipeQuery {
    override fun toReportOptions(context: IQueryGeneratorContext): SegmentationReportOptions {
        val modelName = DbtJinjaRenderer.renderer.renderModelNameRegex(dataset)
        val model = context.getModel(modelName)
        return SegmentationReportOptions(
            modelName,
            dimensions?.map { it.toDimension(modelName, it.getType(context::getModel, modelName)) },
            measures?.map { it.toMeasure(modelName) } ?: listOf(),
            filters?.map { it.toReportFilter(context, modelName) },
            reportOptions,
            limit = limit,
            orders = orders?.entries?.map { order ->
                val fieldModel = if (order.key.relation != null) {
                    model.relations.find { it.name == order.key.relation }!!.modelName
                } else modelName
                val targetModel = context.getModel(fieldModel)
                when {
                    targetModel.dimensions.any { it.name == order.key.name } -> {
                        SegmentationReportOptions.Order(DIMENSION, order.key.toDimension(modelName), order.value == Recipe.OrderType.ASC)
                    }
                    targetModel.measures.any { it.name == order.key.name } -> {
                        SegmentationReportOptions.Order(MEASURE, order.key.toMeasure(modelName), order.value == Recipe.OrderType.ASC)
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
        val measures: List<Recipe.MetricReference>,
        val dimensions: List<Recipe.DimensionReference>?,
        val filters: List<OrFilters>?
    ) : MaterializeQuery {
        override fun toQuery(modelName: ModelName): RecipeQuery {
            return SegmentationRecipeQuery(modelName, measures, dimensions, filters)
        }
    }
}
