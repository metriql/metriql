package com.metriql.interfaces

import com.metriql.audit.MetriqlEvents.AuditLog.SQLExecuteEvent.SQLContext
import com.metriql.auth.ProjectAuth
import com.metriql.db.QueryResult
import com.metriql.report.ISqlQueryTaskGenerator
import com.metriql.report.QueryTask
import com.metriql.report.sql.SqlReportOptions
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class TestSqlQueryTaskGenerator : ISqlQueryTaskGenerator {
    override fun createTask(
        auth: ProjectAuth,
        queryGeneratorContext: IQueryGeneratorContext,
        dataSource: DataSource,
        rawSqlQuery: String,
        queryOptions: SqlReportOptions.QueryOptions,
        isBackgroundTask: Boolean,
        postProcessors: List<(QueryResult) -> QueryResult>,
        context: Pair<SQLContext, Any>?
    ): QueryTask {
        val task = dataSource.createQueryTask(
            auth.warehouseAuth(),
            rawSqlQuery,
            queryOptions.defaultSchema,
            queryOptions.defaultDatabase,
            WarehouseQueryTask.MAX_LIMIT,
            isBackgroundTask
        )

        task.addPostProcessor {
            it.setQueryProperties(rawSqlQuery, queryOptions.limit ?: 1000)
            it
        }
        return task
    }
}
