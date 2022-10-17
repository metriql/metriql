package com.metriql.report.data

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.metriql.dbt.DbtJinjaRenderer
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.service.model.ModelName
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

data class Dataset(
    val modelName: String,
    val filters: ReportFilters?,
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    val dimension: ReportMetric.ReportDimension?
) {
    @JsonIgnore
    fun toRecipe(): RecipeDataset {
        return RecipeDataset(modelName, filters?.toRecipeFilters(), dimension?.toReference())
    }
}

data class RecipeDataset(
    @JsonAlias("model")
    val dataset: String,
    val filters: SegmentationRecipeQuery.Filters?,
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    val dimension: Recipe.FieldReference?
) {
    @JsonIgnore
    fun toDataset(context: IQueryGeneratorContext): Dataset {
        val modelName = DbtJinjaRenderer.renderer.renderModelNameRegex(dataset)
        val dimension = dimension?.toDimension(dataset, dimension.getType(context, modelName))
        val model = context.getModel(modelName)
        return Dataset(modelName, filters?.toReportFilters(context, model), dimension)
    }
}

fun getUsedModels(step: Dataset, context: IQueryGeneratorContext): List<ModelName> {
    return listOf(step.modelName)
}
