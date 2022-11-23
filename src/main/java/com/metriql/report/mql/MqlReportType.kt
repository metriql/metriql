package com.metriql.report.mql

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class MqlReportTypeProxy : ReportType by MqlReportType, ReportTypeProxy(MqlReportType)

object MqlReportType : ReportType {
    override val slug = "mql"
    override val dataClass = MqlQuery::class
    override val serviceClass = MqlService::class
}
