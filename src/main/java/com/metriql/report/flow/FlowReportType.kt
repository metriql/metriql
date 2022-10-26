package com.metriql.report.flow

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class FlowReportTypeProxy : ReportType by FlowReportType, ReportTypeProxy(FlowReportType)

object FlowReportType : ReportType {
    override val slug = "flow"
    override val configClass = FlowQuery::class
    override val serviceClass = FlowService::class
}
