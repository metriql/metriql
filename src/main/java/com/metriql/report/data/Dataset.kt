package com.metriql.report.data

import com.fasterxml.jackson.annotation.JsonProperty
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.dataset.DatasetName
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

data class Dataset(
    val dataset: String,
    val filters: FilterValue?,
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    val dimension: Recipe.FieldReference?
) {
    fun getUsedModels(context: IQueryGeneratorContext): List<DatasetName> {
        return listOf(dataset)
    }
}
