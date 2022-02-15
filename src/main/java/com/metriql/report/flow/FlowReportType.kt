package com.metriql.report.flow

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class FlowReportTypeProxy : ReportType by FlowReportType, ReportTypeProxy(FlowReportType)

object FlowReportType : ReportType {
    override val slug = "flow"
    override val configClass = FlowReportOptions::class
    override val recipeClass = FlowReportOptions::class
    override val serviceClass = FlowService::class
}
