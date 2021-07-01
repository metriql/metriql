package com.metriql.report.retention

import com.metriql.report.data.Dataset
import com.metriql.service.model.DimensionName
import com.metriql.util.RPeriod
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions

data class RetentionReportOptions(
    val firstStep: Dataset,
    val returningStep: Dataset,
    val defaultDateRange: RPeriod? = null,
    val dimension: DimensionName?,
    val approximate: Boolean,
    val dateUnit: DateUnit,
    val connector: DimensionName?,
) : ServiceReportOptions {

    @UppercaseEnum
    enum class DateUnit {
        DAY, WEEK, MONTH;
    }

    override fun toRecipeQuery(): RecipeQuery {
        return RetentionRecipeQuery(firstStep.toRecipe(), returningStep.toRecipe(), dimension, approximate, dateUnit, connector)
    }

    override fun getQueryLimit() = WarehouseQueryTask.MAX_LIMIT
}
