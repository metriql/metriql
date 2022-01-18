package com.metriql.report.mql

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class MqlReportTypeProxy : ReportType by MqlReportType, ReportTypeProxy(MqlReportType)

object MqlReportType : ReportType {
    override val slug = "mql"
    override val configClass = MqlReportOptions::class
    override val recipeClass = MqlReportOptions::class
    override val serviceClass = MqlService::class
}
