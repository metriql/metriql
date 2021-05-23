package com.metriql.warehouse.postgresql

import com.fasterxml.jackson.core.type.TypeReference
import com.metriql.auth.ProjectAuth
import com.metriql.report.retention.RetentionReportOptions
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.retention.Retention
import com.metriql.warehouse.spi.services.retention.RetentionQueryGenerator
import com.metriql.warehouse.spi.services.retention.RetentionSupport
import io.netty.handler.codec.http.HttpResponseStatus

class PostgresqlRetentionQueryGenerator : RetentionQueryGenerator {
    override fun generateSQL(auth: ProjectAuth, context: IQueryGeneratorContext, queryDSL: Retention, options: RetentionReportOptions): String {
        val queryNode = JsonHelper.convert(queryDSL, object : TypeReference<Map<String, *>>() {})
        checkSupport(options.approximate, options)
        val template = if (options.approximate) throw MetriqlException("Approximate mode is not supported by postgresql", HttpResponseStatus.BAD_REQUEST) else expensive
        return jinja.render(
            template,
            mapOf(
                "retention" to queryNode,
                "has_view_models" to context.viewModels.isNotEmpty()
            )
        )
    }

    override fun supports(): List<RetentionSupport> {
        return listOf(
            RetentionSupport(
                approximate = false,
                dimension = true
            )
        )
    }

    companion object {
        private val expensive = this::class.java.getResource("/sql/retention/warehouse/postgresql/expensive.jinja2").readText()
    }
}
