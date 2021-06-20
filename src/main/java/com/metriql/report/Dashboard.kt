package com.metriql.dashboard

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver
import com.metriql.db.FieldType
import com.metriql.db.JSONBSerializable
import com.metriql.report.Recipe.DimensionReference
import com.metriql.report.Recipe.ExportDashboard.RecipeFilterSchema
import com.metriql.report.Recipe.MetricReference
import com.metriql.report.Report.ReportUser
import com.metriql.report.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.ReportFilter.FilterValue.MetricFilter.Filter.OperatorTypeResolver
import com.metriql.report.ReportFilter.FilterValue.MetricFilter.MetricType.DIMENSION
import com.metriql.report.ReportFilter.FilterValue.MetricFilter.MetricType.MAPPING_DIMENSION
import com.metriql.report.ReportMetric
import com.metriql.report.ReportType
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.netty.handler.codec.http.HttpResponseStatus
import java.time.Duration
import java.time.Instant
import kotlin.reflect.KClass

data class DashboardFilter(
    val name: String,
    val items: List<FilterItem>,
) {
    data class FilterItem(
        val valueType: FieldType,
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "valueType", include = JsonTypeInfo.As.EXTERNAL_PROPERTY)
        @JsonTypeIdResolver(OperatorTypeResolver::class)
        val operator: Enum<*>,
        val value: Any?,
    )
}

data class Dashboard(
    val id: Int,
    val name: String,
    val description: String?,
    val user: ReportUser,
    val permission: Permission,
    val refreshInterval: Duration?,
    val filterSchema: List<DashboardFilterSchemaItem>,
    val schedule: DashboardSchedule,
    var items: List<DashboardItem>?,
    val recipe: Recipe?,
    val createdAt: Instant?,
    val category: String?,
) {

    @JSONBSerializable
    data class DashboardSchedule(
        val type: Type,
        @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
        val value: DashboardScheduleValue,
    ) {

        @UppercaseEnum
        enum class Type(private val clazz: KClass<out DashboardScheduleValue>) : StrValueEnum {
            ON_DEMAND(DashboardScheduleValue.OnDemandDashboardSchedule::class),
            SCHEDULED(DashboardScheduleValue.ScheduledDashboardSchedule::class);

            override fun getValueClass(): Class<*> {
                return this.clazz.java
            }
        }

        sealed class DashboardScheduleValue {
            data class OnDemandDashboardSchedule(val invalidateInterval: Duration) : DashboardScheduleValue()
            data class ScheduledDashboardSchedule(val cronjob: String) : DashboardScheduleValue()
        }
    }

    data class Recipe(val id: Int, val slug: String, val path: String?)

    @JSONBSerializable
    data class DashboardFilterSchemaItem(
        val name: String,
        val type: MetricFilter.MetricType,
        @PolymorphicTypeStr<MetricFilter.MetricType>(externalProperty = "type", valuesEnum = MetricFilter.MetricType::class)
        val value: ReportMetric,
        val defaultValue: Any?,
        val operation: String?,
        val isRequired: Boolean,
    ) {
        @JsonIgnore
        fun toRecipeDefinition(): RecipeFilterSchema {
            return when (value) {
                is ReportMetric.ReportDimension -> {
                    val dimension = DimensionReference(MetricReference(value.name), JsonHelper.convert(value.postOperation, String::class.java))
                    RecipeFilterSchema(value.modelName, dimension, null, operation, defaultValue, isRequired)
                }
                is ReportMetric.ReportMappingDimension -> {
                    val dimension =
                        DimensionReference(MetricReference(JsonHelper.convert(value.name, String::class.java)), JsonHelper.convert(value.postOperation, String::class.java))
                    RecipeFilterSchema(null, null, dimension, operation, defaultValue, isRequired)
                }
                else -> throw IllegalStateException()
            }
        }

        @JsonIgnore
        fun toFilter(context: IQueryGeneratorContext): MetricFilter {
            return when (value) {
                is ReportMetric.ReportDimension -> {
                    val dimension = context.getModelDimension(value.name, value.modelName!!)
                    val valueType = dimension.dimension.fieldType ?: FieldType.UNKNOWN
                    val operator = valueType.operatorClass?.javaObjectType?.enumConstants?.find { it.name == operation }
                    val metricValue = ReportMetric.ReportDimension(value.name, value.modelName, null, null)

                    val filter = if (operator != null) {
                        MetricFilter.Filter(DIMENSION, metricValue, valueType, operator, defaultValue)
                    } else {
                        null
                    }

                    MetricFilter(DIMENSION, metricValue, listOfNotNull(filter))
                }
                is ReportMetric.ReportMappingDimension -> {
                    val valueType = value.name.fieldType
                    val operator = valueType.operatorClass?.javaObjectType?.enumConstants?.find { it.name == (operation ?: "equals") }.let {
                        when (valueType) {
                            FieldType.TIMESTAMP -> {
                                TimestampOperatorType.BETWEEN
                            }
                            else -> null
                        }
                    }
                    val metricValue = ReportMetric.ReportMappingDimension(value.name, null)

                    val filter = if (operator != null) {
                        MetricFilter.Filter(MAPPING_DIMENSION, metricValue, valueType, operator, defaultValue)
                    } else {
                        null
                    }

                    MetricFilter(MAPPING_DIMENSION, metricValue, listOfNotNull(filter))
                }
                else -> throw IllegalStateException()
            }
        }
    }

    data class Permission(val everyone: Boolean, val groups: List<Int>?) {
        init {
            if (everyone && groups?.isEmpty() == false) {
                throw MetriqlException("permission.groups must be null if permission.everyone is true", HttpResponseStatus.BAD_REQUEST)
            }
        }
    }

    data class DashboardItem constructor(
        val id: Int?,
        val name: String,
        val description: String?,
        val ttl: Duration?,
        val lastQueryDuration: Duration?,
        val lastUpdated: Instant?,
        val x: Int?,
        val y: Int?,
        val h: Int,
        val w: Int,
        val component: String,
        val reportType: ReportType,
        @PolymorphicTypeStr<ReportType>(externalProperty = "reportType", valuesEnum = ReportType::class)
        val reportOptions: ServiceReportOptions,
        val modelCategory: String?,
    ) {
        init {
            if (h < 1) {
                throw MetriqlException("h can't be less than 1", HttpResponseStatus.BAD_REQUEST)
            }

            if (w < 1) {
                throw MetriqlException("h can't be less than 1", HttpResponseStatus.BAD_REQUEST)
            }
        }
    }
}
