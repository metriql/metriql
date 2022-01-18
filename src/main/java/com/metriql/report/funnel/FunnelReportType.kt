package com.metriql.report.funnel

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class FunnelReportTypeProxy : ReportType by FunnelReportType, ReportTypeProxy(FunnelReportType)

object FunnelReportType : ReportType {
    override val slug = "funnel"
    override val configClass = FunnelReportOptions::class
    override val recipeClass = FunnelRecipeQuery::class
    override val serviceClass = FunnelService::class
}
