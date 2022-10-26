package com.metriql.warehouse.spi.services.flow

import com.metriql.report.flow.FlowQuery
import com.metriql.warehouse.spi.services.ServiceQueryDSL
import com.metriql.warehouse.spi.services.ServiceQueryGenerator
import com.metriql.warehouse.spi.services.ServiceSupport

typealias FlowQueryGenerator = ServiceQueryGenerator<Flow, FlowQuery, ServiceSupport>

data class Flow(val allEventsReference: String) : ServiceQueryDSL
