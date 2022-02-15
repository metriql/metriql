package com.metriql.report.flow

import com.metriql.report.data.Dataset
import com.metriql.report.funnel.FunnelReportOptions
import com.metriql.service.model.DimensionName
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions

data class FlowReportOptions(
    val isStartingEvent: Boolean,
    val event: Dataset,
    val events: List<Dataset>,
    val connector: DimensionName?,
    val stepCount: Int,
    val window: FunnelReportOptions.FunnelWindow?
) : ServiceReportOptions, RecipeQuery {
    override fun toRecipeQuery(): RecipeQuery {
        return this
    }

    override fun toReportOptions(context: IQueryGeneratorContext): ServiceReportOptions {
        return this
    }
}
