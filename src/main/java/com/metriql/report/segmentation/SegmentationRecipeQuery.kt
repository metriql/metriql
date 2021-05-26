package com.metriql.report.segmentation

import com.fasterxml.jackson.annotation.JsonAlias
import com.metriql.dbt.DbtJinjaRenderer
import com.metriql.report.Recipe
import com.metriql.report.segmentation.SegmentationReportOptions.Order.Type.MEASURE
import com.metriql.service.model.ModelName
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.MaterializeQuery
import com.metriql.warehouse.spi.services.RecipeQuery

data class SegmentationRecipeQuery(
    @JsonAlias("model")
    val dataset: ModelName,
    val measures: List<Recipe.MetricReference>,
    val dimensions: List<Recipe.DimensionReference>?,
    val filters: List<Recipe.FilterReference>?,
    val reportOptions: SegmentationReportOptions.ReportOptions? = null,
    val limit: Int? = null,
    val orders: Map<Recipe.MetricReference, Recipe.OrderType>? = null
) : RecipeQuery {
    override fun toReportOptions(context: IQueryGeneratorContext): SegmentationReportOptions {
        val modelName = DbtJinjaRenderer.renderer.renderModelNameRegex(dataset)
        return SegmentationReportOptions(
            modelName,
            dimensions?.map { it.toDimension(modelName, it.getType(context::getModel, modelName)) },
            measures.map { it.toMeasure(modelName) },
            filters?.map { it.toReportFilter(context, modelName) },
            reportOptions,
            limit = limit,
            orders = orders?.entries?.map {
                SegmentationReportOptions.Order(MEASURE, it.key.toMeasure(modelName), it.value == Recipe.OrderType.ASC)
            }
        )
    }

    override fun toMaterialize(): SegmentationMaterialize {
        return SegmentationMaterialize(measures, dimensions, filters)
    }

    data class SegmentationMaterialize(
        val measures: List<Recipe.MetricReference>,
        val dimensions: List<Recipe.DimensionReference>?,
        val filters: List<Recipe.FilterReference>?
    ) : MaterializeQuery {
        override fun toQuery(modelName: ModelName): RecipeQuery {
            return SegmentationRecipeQuery(modelName, measures, dimensions, filters)
        }
    }
}
