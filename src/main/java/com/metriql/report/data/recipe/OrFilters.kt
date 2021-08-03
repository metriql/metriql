package com.metriql.report.data.recipe

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.metriql.db.FieldType
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.toCamelCase
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus

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

            var (fieldType, metricValue) = when {
                filter.dimension != null -> {
                    val type = filter.dimension.getType(context::getModel, modelName)
                    Pair(type, filter.dimension.toDimension(modelName, type))
                }
                filter.measure != null -> {
                    Pair(filter.measure.getType(context, modelName), filter.measure.toMeasure(modelName))
                }
                filter.mapping != null -> {
                    val type = JsonHelper.convert(filter.mapping, Model.MappingDimensions.CommonMappings::class.java)
                    Pair(type.fieldType, ReportMetric.ReportMappingDimension(type, null))
                }
                else -> {
                    throw IllegalStateException("One of dimension, measure or mapping is required")
                }
            }

            try {
                JsonHelper.convert(filter.operator, FieldType.UNKNOWN.operatorClass.java)
                fieldType = FieldType.UNKNOWN
            } catch (e: Exception) {
                null
            }

            val operatorBean: Enum<*> = try {
                JsonHelper.convert(filter.operator, fieldType.operatorClass.java)
            } catch (e: Exception) {
                val values = fieldType.operatorClass.java.enumConstants.joinToString(", ") { toCamelCase(it.name) }
                throw MetriqlException(
                    "Invalid operator `${filter.operator}`, available values for type $fieldType is $values",
                    HttpResponseStatus.BAD_REQUEST
                )
            }

            ReportFilter.FilterValue.MetricFilter.Filter(metricType, metricValue, fieldType, operatorBean, filter.value)
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
