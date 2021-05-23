package com.metriql.warehouse.spi.services.flow

import com.metriql.report.flow.FlowReportOptions
import com.metriql.warehouse.spi.services.ServiceQueryDSL
import com.metriql.warehouse.spi.services.ServiceQueryGenerator
import com.metriql.warehouse.spi.services.ServiceSupport

typealias FlowQueryGenerator = ServiceQueryGenerator<Flow, FlowReportOptions, ServiceSupport>

data class Flow(val allEventsReference: String) : ServiceQueryDSL
