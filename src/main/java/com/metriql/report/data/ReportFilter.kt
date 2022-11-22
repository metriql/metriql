package com.metriql.report.data

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.util.JsonParserSequence
import com.fasterxml.jackson.databind.DatabindContext
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.metriql.db.FieldType
import com.metriql.db.JSONBSerializable
import com.metriql.report.data.ReportFilter.FilterValue.MetricFilter.Connector
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.dataset.Dataset
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.util.getOperation
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import io.netty.handler.codec.http.HttpResponseStatus
import kotlin.reflect.KClass

@JSONBSerializable
@JsonDeserialize(using = ReportFilter.FilterValueJsonDeserializer::class)
data class ReportFilter(
    val value: FilterValue
) {
    class FilterValueJsonDeserializer : JsonDeserializer<ReportFilter>() {
        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ReportFilter {
            return if (p.isExpectedStartObjectToken) {
                val tb = ctxt.bufferForInputBuffering(p)

                val nextFieldName = p.nextFieldName()
                val connector = try {
                    JsonHelper.convert(nextFieldName, Connector::class.java)
                } catch (e: Exception) {
                    null
                }

                if (connector != null) {
                    p.nextToken()
                    val filters: List<ReportFilter> = p.readValueAs(object : com.fasterxml.jackson.core.type.TypeReference<List<ReportFilter>>() {})
                    p.nextToken()
                    ReportFilter(FilterValue.NestedFilter(connector, filters))
                } else {
                    tb.copyCurrentStructure(p)
                    p.clearCurrentToken()
                    val parser = JsonParserSequence.createFlattened(false, tb.asParser(p), p)
                    val filter = parser.readValueAs(FilterValue.MetricFilter.Filter::class.java)
                    ReportFilter(FilterValue.MetricFilter(Connector.AND, listOf(filter)))
                }
            } else if (p.isExpectedStartArrayToken) {
                val filters: List<ReportFilter> = p.readValueAs(object : com.fasterxml.jackson.core.type.TypeReference<List<ReportFilter>>() {})
                ReportFilter(FilterValue.NestedFilter(Connector.AND, filters))
            } else {
                throw ctxt.wrongTokenException(p, ReportFilter::class.java, JsonToken.START_OBJECT, "The filter must be an array or an object")
            }
        }
    }

    sealed class FilterValue {
        data class SqlFilter(val sql: String) : FilterValue() {
            override fun subtract(filter: ReportFilter): ReportFilter? {
                return if (filter.value == this) {
                    null
                } else {
                    filter
                }
            }
        }

        data class NestedFilter(val connector: Connector, val filters: List<ReportFilter>) : FilterValue() {
            override fun subtract(filter: ReportFilter): ReportFilter? {
                return if (filter.value == this) {
                    null
                } else {
                    filter
                }
            }
        }

        data class MetricFilter(val connector: Connector, val filters: List<Filter>) : FilterValue() {
            @UppercaseEnum
            enum class Connector { AND, OR }

            override fun subtract(filter: ReportFilter): ReportFilter? {
                return if (filter.value == this) {
                    null
                } else {
                    filter
                }
            }

            data class Filter(
                @JsonAlias("dimension", "measure", "mapping")
                val metric: Recipe.FieldReference,
                val operator: String,
                val value: Any?
            ) {
                class OperatorTypeResolver : JsonHelper.SimpleTypeResolver() {
                    override fun typeFromId(p0: DatabindContext?, value: String): JavaType {
                        val operatorClass = JsonHelper.convert(value, FieldType::class.java)?.operatorClass?.java
                            ?: throw MetriqlException("No filter available for this type", HttpResponseStatus.BAD_REQUEST)
                        return JsonHelper.getMapper().constructType(operatorClass)
                    }
                }
            }

            @UppercaseEnum
            enum class MetricType(private val clazz: KClass<out ReportMetric>) : StrValueEnum {
                DIMENSION(ReportMetric.ReportDimension::class),
                MAPPING_DIMENSION(ReportMetric.ReportMappingDimension::class),
                MEASURE(ReportMetric.ReportMeasure::class);

                override fun getValueClass() = clazz.java
            }
        }

        // returns the same instance of ReportFilter if this doesn't include the filter passed as an argument
        // returns null if the filters are equal
        // returns a new ReportFilter in case if this includes the filter.
        abstract fun subtract(filter: ReportFilter): ReportFilter?
    }

    companion object {
        fun extractDateRangeForEventTimestamp(filter: ReportFilter?): DateRange? {
            if (filter == null) return null
            // todo: process nested filter as well
            val dateRange: FilterValue.MetricFilter.Filter? = if (filter.value is FilterValue.MetricFilter) {
                filter.value.filters.find {
                    it.metric.getMappingDimensionIfApplicable() == Dataset.MappingDimensions.CommonMappings.TIME_SERIES.name.lowercase()
                }
            } else null

            return dateRange?.let {
                val (type, operation) = getOperation(FieldType.TIMESTAMP, it.operator)
                ANSISQLFilters.convertTimestampFilterToDates(operation as TimestampOperatorType, it.value)
            }
        }
    }
}
