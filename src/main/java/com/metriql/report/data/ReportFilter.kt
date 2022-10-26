package com.metriql.report.data

import com.fasterxml.jackson.databind.DatabindContext
import com.fasterxml.jackson.databind.JavaType
import com.metriql.db.FieldType
import com.metriql.db.JSONBSerializable
import com.metriql.report.data.recipe.OrFilters
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.model.Dataset
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.util.getOperation
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import io.netty.handler.codec.http.HttpResponseStatus
import kotlin.reflect.KClass

@JSONBSerializable
data class ReportFilter(
    val type: Type,
    @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
    val value: FilterValue
) {

    @UppercaseEnum
    enum class Type(private val clazz: KClass<out FilterValue>) : StrValueEnum {
        SQL(FilterValue.SqlFilter::class),
        NESTED(FilterValue.NestedFilter::class),
        METRIC(FilterValue.MetricFilter::class);

        override fun getValueClass() = clazz.java
    }

    fun toReference(): OrFilters? {
        return when (value) {
            is FilterValue.SqlFilter -> throw UnsupportedOperationException()
            is FilterValue.MetricFilter -> {
                if (value.filters.isEmpty()) {
                    null
                } else {
                    val references = OrFilters()
                    value.filters.map {
                        val item = when (val metricValue = it.metric) {
                            is ReportMetric.ReportDimension ->
                                Recipe.FilterReference(dimension = metricValue.toReference(), operator = it.operator, value = it.value)

                            is ReportMetric.ReportMeasure ->
                                Recipe.FilterReference(measure = metricValue.toMetricReference(), operator = it.operator, value = it.value)

                            is ReportMetric.ReportMappingDimension ->
                                Recipe.FilterReference(mapping = metricValue.name.name, operator = it.operator, value = it.value)

                            is ReportMetric.Function -> TODO()
                            is ReportMetric.Unary -> TODO()
                        }

                        references.add(item)
                    }

                    references
                }
            }

            is FilterValue.NestedFilter -> TODO()
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

        data class NestedFilter(val connector: MetricFilter.Connector, val filters: List<ReportFilter>) : FilterValue() {
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
                val type: MetricType,
                @PolymorphicTypeStr<MetricType>(externalProperty = "type", valuesEnum = MetricType::class)
                val metric: ReportMetric,
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
                UNARY(ReportMetric.Unary::class),
                FUNCTION(ReportMetric.ReportDimension::class),
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
            if(filter == null) return null
            // todo: process nested filter as well
            val dateRange: FilterValue.MetricFilter.Filter? = if (filter.value is FilterValue.MetricFilter) {
                filter.value.filters.find {
                        it.metric is ReportMetric.ReportMappingDimension &&
                        it.metric.name == Dataset.MappingDimensions.CommonMappings.TIME_SERIES
                }
            } else null

            return dateRange?.let {
                val (type, operation) = getOperation(FieldType.TIMESTAMP, it.operator)
                ANSISQLFilters.convertTimestampFilterToDates(operation as TimestampOperatorType, it.value)
            }
        }
    }
}
