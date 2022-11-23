package com.metriql.report.data.recipe

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.metriql.report.data.FilterValue
import com.metriql.service.dataset.DatasetName
import com.metriql.util.JsonUtil
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

// Depends on ACCEPT_SINGLE_VALUE_AS_ARRAY
@JsonSerialize(using = OrFilters.FilterReferenceSerializer::class)
class OrFilters : ArrayList<Recipe.FilterReference>() {
    @JsonIgnore
    fun toReportFilter(
        context: IQueryGeneratorContext,
        datasetName: DatasetName,
    ): FilterValue {
        val orFilters = map { filter ->
            filter.toFilter(context, datasetName)
        }

        return FilterValue.NestedFilter(
            FilterValue.NestedFilter.Connector.OR,
            orFilters
        )
    }

    class FilterReferenceSerializer : JsonSerializer<OrFilters>() {
        override fun serialize(value: OrFilters, gen: JsonGenerator, serializers: SerializerProvider) {
            JsonUtil.serializeAsArrayOrSingle(value, gen, serializers, Recipe.FilterReference::class.java)
        }
    }
}
