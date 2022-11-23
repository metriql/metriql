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

@JsonDeserialize(using = FilterValue.FilterValueJsonDeserializer::class)
sealed class FilterValue {
    class FilterValueJsonDeserializer : JsonDeserializer<FilterValue>() {
        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): FilterValue {
            return if (p.isExpectedStartObjectToken) {
                val tb = ctxt.bufferForInputBuffering(p)

                val nextFieldName = p.nextFieldName()
                val connector = try {
                    JsonHelper.convert(nextFieldName, NestedFilter.Connector::class.java)
                } catch (e: Exception) {
                    null
                }

                if (connector != null) {
                    p.nextToken()
                    val filters: List<FilterValue> = p.readValueAs(object : com.fasterxml.jackson.core.type.TypeReference<List<FilterValue>>() {})
                    p.nextToken()
                    NestedFilter(connector, filters)
                } else {
                    tb.copyCurrentStructure(p)
                    p.clearCurrentToken()
                    val parser = JsonParserSequence.createFlattened(false, tb.asParser(p), p)
                    val filter = parser.readValueAs(MetricFilter::class.java)
                    filter
                }
            } else if (p.isExpectedStartArrayToken) {
                val filters: List<FilterValue> = p.readValueAs(object : com.fasterxml.jackson.core.type.TypeReference<List<FilterValue>>() {})
                NestedFilter(NestedFilter.Connector.AND, filters)
            } else {
                throw ctxt.wrongTokenException(p, FilterValue::class.java, JsonToken.START_OBJECT, "The filter must be an array or an object")
            }
        }
    }

    @JsonDeserialize(using = JsonDeserializer.None::class)
    data class SqlFilter(val sql: String) : FilterValue() {
        override fun subtract(filter: FilterValue): FilterValue? {
            return if (filter == this) {
                null
            } else {
                filter
            }
        }
    }

    @JsonDeserialize(using = JsonDeserializer.None::class)
    data class NestedFilter(val connector: Connector, val filters: List<FilterValue>) : FilterValue() {
        override fun subtract(filter: FilterValue): FilterValue? {
            return if (filter == this) {
                null
            } else {
                filter
            }
        }

        @UppercaseEnum
        enum class Connector { AND, OR }
    }

    // returns the same instance of ReportFilter if this doesn't include the filter passed as an argument
    // returns null if the filters are equal
    // returns a new ReportFilter in case if this includes the filter.
    abstract fun subtract(filter: FilterValue): FilterValue?

    // required for Jackson to ignore the FilterValueJsonDeserializer
    @JsonDeserialize(using = JsonDeserializer.None::class)
    data class MetricFilter(
        @JsonAlias("dimension", "measure", "mapping")
        val metric: Recipe.FieldReference,
        val operator: String,
        val value: Any?
    ) : FilterValue() {
        class OperatorTypeResolver : JsonHelper.SimpleTypeResolver() {
            override fun typeFromId(p0: DatabindContext?, value: String): JavaType {
                val operatorClass = JsonHelper.convert(value, FieldType::class.java)?.operatorClass?.java
                    ?: throw MetriqlException("No filter available for this type", HttpResponseStatus.BAD_REQUEST)
                return JsonHelper.getMapper().constructType(operatorClass)
            }
        }

        override fun subtract(filter: FilterValue): FilterValue? {
            return if (filter == this) {
                null
            } else {
                filter
            }
        }
    }

    @UppercaseEnum
    enum class MetricType(private val clazz: KClass<out ReportMetric>) : StrValueEnum {
        DIMENSION(ReportMetric.ReportDimension::class),
        MEASURE(ReportMetric.ReportMeasure::class);

        override fun getValueClass() = clazz.java
    }

    companion object {
        fun extractDateRangeForEventTimestamp(filter: FilterValue?): DateRange? {
            if (filter == null) return null
            // todo: process nested filter as well
            val dateRange: MetricFilter? =
                if (filter is MetricFilter && filter.metric.getMappingDimensionIfApplicable() == Dataset.MappingDimensions.CommonMappings.TIME_SERIES.name.lowercase()) {
                    filter
                } else null

            return dateRange?.let {
                val (type, operation) = getOperation(FieldType.TIMESTAMP, it.operator)
                ANSISQLFilters.convertTimestampFilterToDates(operation as TimestampOperatorType, it.value)
            }
        }
    }
}
