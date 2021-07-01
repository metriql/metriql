package com.metriql.report.data

import com.metriql.report.ReportType
import com.metriql.util.PolymorphicTypeStr
import com.metriql.warehouse.spi.services.ServiceReportOptions
import java.time.Instant

data class Report(
    val id: Int,
    val type: ReportType,
    val user: ReportUser,
    val name: String,
    val createdAt: Instant,
    val description: String?,
    val category: String?,
    val permission: Dashboard.Permission,
    val modelCategory: String?,
    @PolymorphicTypeStr<ReportType>(externalProperty = "type", valuesEnum = ReportType::class)
    val options: ServiceReportOptions
) {
    data class ReportUser(val id: Int, val name: String?, val email: String?)
}
