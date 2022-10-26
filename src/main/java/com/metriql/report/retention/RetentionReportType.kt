package com.metriql.report.retention

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class RetentionReportTypeProxy : ReportType by RetentionReportType, ReportTypeProxy(RetentionReportType)

object RetentionReportType : ReportType {
    override val slug = "retention"
    override val configClass = RetentionQuery::class
    override val serviceClass = RetentionService::class
}
