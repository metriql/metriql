package com.metriql.report.flow

import com.metriql.report.data.Dataset
import com.metriql.report.funnel.FunnelQuery
import com.metriql.service.model.DimensionName
import com.metriql.warehouse.spi.services.ServiceQuery

data class FlowQuery(
    val isStartingEvent: Boolean,
    val event: Dataset,
    val events: List<Dataset>,
    val connector: DimensionName?,
    val stepCount: Int,
    val window: FunnelQuery.FunnelWindow?
) : ServiceQuery
