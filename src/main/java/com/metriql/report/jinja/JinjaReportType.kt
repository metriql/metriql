package com.metriql.report.jinja

import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class JinjaReportTypeProxy : ReportType by JinjaReportType, ReportTypeProxy(JinjaReportType)

object JinjaReportType : ReportType {
    override val slug = "app"
    override val dataClass = ObjectNode::class
    override val serviceClass = JinjaService::class
}
