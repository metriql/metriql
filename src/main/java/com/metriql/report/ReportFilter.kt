package com.metriql.report

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DatabindContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver
import com.metriql.db.FieldType
import com.metriql.db.JSONBSerializable
import com.metriql.service.model.Model
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.util.toCamelCase
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
        SQL_FILTER(FilterValue.Sql::class),
        METRIC_FILTER(FilterValue.MetricFilter::class);

        override fun getValueClass() = clazz.java
    }

    fun toReference(): Recipe.FilterReference? {
        return when (value) {
            is FilterValue.Sql -> throw java.lang.IllegalStateException()
            is FilterValue.MetricFilter -> {
                if (value.filters.isEmpty()) {
                    null
                } else {
                    val firstFilter = value.filters[0]

                    val operator = toCamelCase(firstFilter.operator.name)

                    when (value.metricValue) {
                        is ReportMetric.ReportDimension ->
                            Recipe.FilterReference(dimension = value.metricValue.toReference(), operator = operator, value = firstFilter.value)
                        is ReportMetric.ReportMeasure ->
                            Recipe.FilterReference(measure = value.metricValue.toMetricReference(), operator = operator, value = firstFilter.value)
                        is ReportMetric.ReportMappingDimension ->
                            Recipe.FilterReference(mapping = value.metricValue.name.name, operator = operator, value = firstFilter.value)
                        is ReportMetric.Function -> TODO()
                        is ReportMetric.Unary -> TODO()
                    }
                }
            }
        }
    }

    sealed class FilterValue {
        data class Sql(val sql: String) : FilterValue() {

            override fun subtract(filter: ReportFilter): ReportFilter? {
                return if (filter.value == this) {
                    null
                } else {
                    filter
                }
            }
        }

        data class MetricFilter(
            val metricType: MetricType,
            @PolymorphicTypeStr<MetricType>(externalProperty = "metricType", valuesEnum = MetricType::class)
            val metricValue: ReportMetric,
            val filters: List<Filter>
        ) : FilterValue() {

            override fun subtract(filter: ReportFilter): ReportFilter? {
                return if (filter.value == this) {
                    null
                } else {
                    filter
                }
            }

            data class Filter(
                val valueType: FieldType,
                @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "valueType", include = JsonTypeInfo.As.EXTERNAL_PROPERTY)
                @JsonTypeIdResolver(OperatorTypeResolver::class)
                val operator: Enum<*>,
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
                LITERAL(ReportMetric.Function::class),
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
        fun extractDateRangeForEventTimestamp(filters: List<ReportFilter>): DateRange? {
            return filters.mapNotNull {
                if (it.value is FilterValue.MetricFilter &&
                    it.value.metricType == FilterValue.MetricFilter.MetricType.MAPPING_DIMENSION &&
                    it.value.metricValue is ReportMetric.ReportMappingDimension &&
                    it.value.metricValue.name == Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP
                ) {
                    it.value.filters[0]
                } else null
            }.firstOrNull()?.let {
                ANSISQLFilters.convertTimestampFilterToDates(it.operator as TimestampOperatorType, it.value)
            }
        }
    }
}
