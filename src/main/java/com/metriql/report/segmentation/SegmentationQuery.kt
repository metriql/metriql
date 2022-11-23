package com.metriql.report.segmentation

import com.metriql.report.data.FilterValue
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.dataset.DatasetName
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.RPeriod
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.services.ServiceQuery
import io.netty.handler.codec.http.HttpResponseStatus
import kotlin.reflect.KClass

data class SegmentationQuery(
    val dataset: DatasetName,
    val dimensions: List<Recipe.FieldReference>?,
    val measures: List<Recipe.FieldReference>?,
    val filters: FilterValue? = null,
    val defaultDateRange: RPeriod? = null,
    val limit: Int? = null,
    val orders: Map<Recipe.FieldReference, Recipe.OrderType>? = null
) : ServiceQuery() {
    init {
        if (limit != null && (limit < 0 || limit > WarehouseQueryTask.MAX_LIMIT)) {
            throw MetriqlException("Segmentation limit can not be less than 0 or more than ${WarehouseQueryTask.MAX_LIMIT}", HttpResponseStatus.BAD_REQUEST)
        }
    }

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
}
