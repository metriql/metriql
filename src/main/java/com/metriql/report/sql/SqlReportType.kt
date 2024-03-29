package com.metriql.report.sql

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class SqlReportTypeProxy : ReportType by SqlReportType, ReportTypeProxy(SqlReportType)

object SqlReportType : ReportType {
    override val slug = "sql"
    override val dataClass = SqlQuery::class
    override val serviceClass = SqlService::class
}
