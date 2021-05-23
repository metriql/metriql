package com.metriql.warehouse.bigquery

import com.fasterxml.jackson.core.type.TypeReference
import com.metriql.auth.ProjectAuth
import com.metriql.report.retention.RetentionReportOptions
import com.metriql.util.JsonHelper
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.retention.Retention
import com.metriql.warehouse.spi.services.retention.RetentionQueryGenerator
import com.metriql.warehouse.spi.services.retention.RetentionSupport

class BigQueryRetentionQueryGenerator : RetentionQueryGenerator {
    override fun generateSQL(auth: ProjectAuth, context: IQueryGeneratorContext, queryDSL: Retention, options: RetentionReportOptions): String {
        checkSupport(options.approximate, options)
        return jinja.render(
            approximate,
            mapOf(
                "retention" to JsonHelper.convert(queryDSL, object : TypeReference<Map<String, *>>() {}),
                "has_view_models" to context.viewModels.isNotEmpty()
            )
        )
    }

    override fun supports(): List<RetentionSupport> {
        return listOf(
            RetentionSupport(
                approximate = true,
                // TODO: the query returns multiple rows for each dimension so it's disabled for now
                dimension = false
            )
        )
    }

    companion object {
        private val approximate = this::class.java.getResource("/sql/retention/warehouse/bigquery/approximate.jinja2").readText()
    }
}
