package com.metriql.report.segmentation

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.Recipe
import com.metriql.model.ModelName
import com.metriql.report.ReportFilter
import com.metriql.report.ReportMetric
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.RPeriod
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.netty.handler.codec.http.HttpResponseStatus
import kotlin.reflect.KClass

data class SegmentationReportOptions(
    val modelName: ModelName,
    val dimensions: List<ReportMetric.ReportDimension>?,
    val measures: List<ReportMetric.ReportMeasure>,
    val filters: List<ReportFilter>? = null,
    val reportOptions: ReportOptions? = null,
    val defaultDateRange: RPeriod? = null,
    val limit: Int? = null,
    val orders: List<Order>? = null
) : ServiceReportOptions {
    init {
        if (limit != null && (limit < 1 || limit > WarehouseQueryTask.MAX_LIMIT)) {
            throw MetriqlException("Segmentation limit can not be less than 1 or more than ${WarehouseQueryTask.MAX_LIMIT}", HttpResponseStatus.BAD_REQUEST)
        }
    }

    override fun getQueryLimit(): Int? = limit

    data class Order(
        val type: Type,
        @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
        val value: ReportMetric,
        val ascending: Boolean? = true
    ) {
        @UppercaseEnum
        enum class Type(private val clazz: KClass<out ReportMetric>) : StrValueEnum {
            MEASURE(ReportMetric.ReportMeasure::class), DIMENSION(ReportMetric.ReportDimension::class);

            override fun getValueClass() = clazz.java
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true) // TODO REMOVE THIS
    data class ReportOptions(
        val chartOptions: ObjectNode?,
        val tableOptions: ObjectNode?,
        val columnOptions: ObjectNode?
    )

    override fun toRecipeQuery(): RecipeQuery {
        return SegmentationRecipeQuery(
            modelName,
            measures.map { it.toMetricReference() },
            dimensions?.map { it.toReference() },
            filters?.mapNotNull { it.toReference() },
            reportOptions, limit,
            orders?.map { it.value.toMetricReference() to if (it.ascending == true) Recipe.OrderType.ASC else Recipe.OrderType.DESC }?.toMap()
        )
    }
}
