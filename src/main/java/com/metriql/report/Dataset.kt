package com.metriql.report

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.metriql.dbt.DbtJinjaRenderer
import com.metriql.service.model.ModelName
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

data class Dataset(
    val modelName: String,
    val filters: List<ReportFilter>,
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    val dimension: ReportMetric.ReportDimension?
) {
    @JsonIgnore
    fun toRecipe(): RecipeDataset {
        return RecipeDataset(modelName, filters.mapNotNull { it.toReference() }, dimension?.toReference())
    }
}

data class RecipeDataset(
    @JsonAlias("model")
    val dataset: String,
    val filters: List<Recipe.FilterReferences>?,
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    val dimension: Recipe.DimensionReference?
) {
    @JsonIgnore
    fun toDataset(context: IQueryGeneratorContext): Dataset {
        val modelName = DbtJinjaRenderer.renderer.renderModelNameRegex(dataset)
        val filters = filters?.map { it.toReportFilter(context, modelName) } ?: listOf()
        val dimension = dimension?.toDimension(dataset, dimension.getType(context::getModel, modelName))
        return Dataset(modelName, filters, dimension)
    }
}

fun getUsedModels(step: Dataset, context: IQueryGeneratorContext): List<ModelName> {
    val filterRelations = step.filters.mapNotNull {
        when (it.value) {
            is ReportFilter.FilterValue.MetricFilter -> it.value.metricValue?.toMetricReference()?.relation
            is ReportFilter.FilterValue.Sql -> null
        }
    }

    val model = context.getModel(step.modelName)

    val relationModels = filterRelations.map { relation -> model.relations.first { it.name == relation }.modelName }
    return relationModels + listOf(step.modelName)
}
