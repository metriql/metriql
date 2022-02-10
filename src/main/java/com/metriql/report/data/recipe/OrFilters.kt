package com.metriql.report.data.recipe

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName
import com.metriql.util.JsonHelper
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

// Depends on ACCEPT_SINGLE_VALUE_AS_ARRAY
@JsonSerialize(using = OrFilters.FilterReferenceSerializer::class)
class OrFilters : ArrayList<Recipe.FilterReference>() {
    @JsonIgnore
    fun toReportFilter(
        context: IQueryGeneratorContext,
        modelName: ModelName,
    ): ReportFilter {
        val orFilters = map { filter ->
            val metricType = when {
                filter.dimension != null -> ReportFilter.FilterValue.MetricFilter.MetricType.DIMENSION
                filter.measure != null -> ReportFilter.FilterValue.MetricFilter.MetricType.MEASURE
                filter.mapping != null -> ReportFilter.FilterValue.MetricFilter.MetricType.MAPPING_DIMENSION
                else -> throw IllegalStateException("One of dimension, measure or mapping is required")
            }

            var metricValue = when {
                filter.dimension != null ->
                    filter.dimension.toDimension(modelName, filter.dimension.getType(context, modelName))
                filter.measure != null ->
                    filter.measure.toMeasure(modelName)
                filter.mapping != null -> {
                    val type = JsonHelper.convert(filter.mapping, Model.MappingDimensions.CommonMappings::class.java)
                    ReportMetric.ReportMappingDimension(type, null)
                }
                else -> {
                    throw IllegalStateException("One of dimension, measure or mapping is required")
                }
            }
            ReportFilter.FilterValue.MetricFilter.Filter(metricType, metricValue, filter.operator, filter.value)
        }

        return ReportFilter(
            ReportFilter.Type.METRIC_FILTER,
            ReportFilter.FilterValue.MetricFilter(
                null, null,
                orFilters
            )
        )
    }

    class FilterReferenceSerializer : JsonSerializer<OrFilters>() {
        override fun serialize(value: OrFilters, gen: JsonGenerator, serializers: SerializerProvider) {
            val serializer = serializers.findValueSerializer(Recipe.FilterReference::class.java)

            when {
                value.size > 1 -> {
                    gen.writeStartArray(value)
                    value.forEach { serializer.serialize(it, gen, serializers) }
                    gen.writeEndArray()
                }
                value.size == 1 -> serializer.serialize(value[0], gen, serializers)
                else -> gen.writeNull()
            }
        }
    }
}
