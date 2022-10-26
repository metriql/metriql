package com.metriql.report

import com.metriql.util.MetriqlException
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.ServiceLoader

object ReportLocator {
    private var services: List<ReportType> = ServiceLoader.load(ReportType::class.java).toList()

    @JvmStatic
    fun getReportType(slug: String): ReportType {
        val service = services.find { it.slug == slug }
        return service ?: throw MetriqlException("Unknown report type: $slug", HttpResponseStatus.BAD_REQUEST)
    }

    fun getList() = services

    @JvmStatic
    fun reload() {
        services = ServiceLoader.load(ReportType::class.java).toList()
    }
}
