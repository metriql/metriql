package com.metriql.report.mql

import com.metriql.report.PostProcessor
import com.metriql.report.QueryTask
import com.metriql.report.QueryTaskGenerator
import com.metriql.report.sql.SqlQuery
import com.metriql.service.audit.MetriqlEvents
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jdbc.LightweightQueryRunner
import com.metriql.util.JsonHelper
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.airlift.jaxrs.testing.GuavaMultivaluedMap
import io.trino.server.HttpRequestSessionContext
import io.trino.spi.security.Identity
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Optional
import javax.ws.rs.core.MultivaluedMap

class MqlQueryTaskGenerator(val runner: LightweightQueryRunner) : QueryTaskGenerator {
    override fun createTask(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        dataSource: DataSource,
        rawSqlQuery: String,
        queryOptions: SqlQuery.QueryOptions,
        isBackgroundTask: Boolean,
        postProcessors: List<PostProcessor>,
        info: MetriqlEvents.AuditLog.SQLExecuteEvent.SQLContext?
    ): QueryTask {
        val headerMap: MultivaluedMap<String, String> = GuavaMultivaluedMap()
        val encode = JsonHelper.encode(auth)
        val context = HttpRequestSessionContext(
            headerMap, Optional.of("Presto"), null,
            Optional.of(Identity.Builder("default").withExtraCredentials(mapOf("metriql" to encode)).build()), runner.groupProviderManager
        )
        return runner.createTask(auth, context, rawSqlQuery)
    }
}
