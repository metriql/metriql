package com.metriql.report.segmentation

import com.fasterxml.jackson.annotation.JsonAlias
import com.metriql.Recipe
import com.metriql.model.ModelName
import com.metriql.report.segmentation.SegmentationReportOptions.Order.Type.MEASURE
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
        return SegmentationReportOptions(
            dataset,
            dimensions?.map { it.toDimension(dataset, it.getType(context::getModel, dataset)) },
            measures.map { it.toMeasure(dataset) },
            filters?.map { it.toReportFilter(context, dataset) },
            reportOptions,
            limit = limit,
            orders = orders?.entries?.map {
                SegmentationReportOptions.Order(MEASURE, it.key.toMeasure(dataset), it.value == Recipe.OrderType.ASC)
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
