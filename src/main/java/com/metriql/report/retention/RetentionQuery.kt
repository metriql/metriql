package com.metriql.report.retention

import com.metriql.report.data.Dataset
import com.metriql.service.model.DimensionName
import com.metriql.util.RPeriod
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.services.ServiceQuery

data class RetentionQuery(
    val firstStep: Dataset,
    val returningStep: Dataset,
    val defaultDateRange: RPeriod? = null,
    val dimension: DimensionName?,
    val approximate: Boolean,
    val dateUnit: DateUnit,
    val connector: DimensionName?,
) : ServiceQuery {

    @UppercaseEnum
    enum class DateUnit {
        DAY, WEEK, MONTH;
    }

    override fun getQueryLimit() = WarehouseQueryTask.MAX_LIMIT
}
