package com.metriql.report

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
    val sharedEveryone: Boolean?,
    val modelCategory: String?,
    @PolymorphicTypeStr<ReportType>(externalProperty = "type", valuesEnum = ReportType::class)
    val options: ServiceReportOptions
) {
    data class ReportUser(val id: Int, val name: String?, val email: String?)
}
