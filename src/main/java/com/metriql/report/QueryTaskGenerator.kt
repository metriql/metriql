package com.metriql.report

import com.metriql.report.sql.SqlQuery
import com.metriql.service.audit.MetriqlEvents
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

interface QueryTaskGenerator {
    fun createTask(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        dataSource: DataSource,
        rawSqlQuery: String,
        queryOptions: SqlQuery.QueryOptions,
        isBackgroundTask: Boolean,
        postProcessors: List<PostProcessor> = listOf(),
        info: MetriqlEvents.AuditLog.SQLExecuteEvent.SQLContext? = null,
    ): QueryTask
}
