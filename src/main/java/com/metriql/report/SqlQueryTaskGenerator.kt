package com.metriql.report

import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.QueryStats
import com.metriql.report.sql.SqlReportOptions
import com.metriql.service.audit.MetriqlEvents.AuditLog.SQLExecuteEvent
import com.metriql.service.audit.MetriqlEvents.AuditLog.SQLExecuteEvent.SQLContext
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.cache.ICacheService
import com.metriql.service.cache.ICacheService.EntityType.SQL_QUERY
import com.metriql.service.task.Task
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlEventBus
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import javax.inject.Inject

typealias QueryTask = Task<QueryResult, QueryStats>
typealias PostProcessor = (QueryResult) -> QueryResult

/*
* This is the last destination of every service.
* Service is responsible for the following:
* 1. Append limit if missing.
* 2. Check if the same query has been executed 60 minutes ago. Return from cache if yes.
* 3. Append viewModels if any available
*   In sql type models, we append their query using WITH alias as (SELECT ..)
*   so that the query is more organized.
* */
class SqlQueryTaskGenerator @Inject constructor(private val cacheService: ICacheService) {
    private val runningTasks = ConcurrentHashMap<QueryIdentifierForRunningTasks, QueryTask>()

    data class QueryIdentifierForCache(
        val query: String,
        val options: SqlReportOptions.QueryOptions,
    )

    data class QueryIdentifierForRunningTasks(
        val projectId: Int,
        val query: String,
        val options: SqlReportOptions.QueryOptions,
    )

    fun createTask(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        dataSource: DataSource,
        rawSqlQuery: String,
        queryOptions: SqlReportOptions.QueryOptions,
        isBackgroundTask: Boolean,
        postProcessors: List<PostProcessor> = listOf(),
        info: Pair<SQLContext, Any>? = null,
    ): QueryTask {
        val limit = if (queryOptions.limit != null && queryOptions.limit > WarehouseQueryTask.MAX_LIMIT) {
            WarehouseQueryTask.MAX_LIMIT
        } else if (queryOptions.limit != null && queryOptions.limit >= 0 && queryOptions.limit <= WarehouseQueryTask.MAX_LIMIT) {
            queryOptions.limit
        } else {
            WarehouseQueryTask.DEFAULT_LIMIT
        }

        val bridge = context.datasource.warehouse.bridge

        val query = bridge.generateQuery(context.viewModels, rawSqlQuery)
        val identifierForTask = QueryIdentifierForRunningTasks(auth.projectId, query, queryOptions)
        val runningTask = runningTasks[identifierForTask]
        if (runningTask != null) {
            return runningTask
        }

        val cacheIdentifier = QueryIdentifierForCache(query, queryOptions)

        if (queryOptions.useCache) {
            val cacheResult = cacheService.getCache(
                ICacheService.CacheKey(
                    auth.projectId,
                    SQL_QUERY,
                    cacheIdentifier
                )
            )

            if (cacheResult != null && !cacheResult.isExpired(TimeUnit.HOURS.toSeconds(1))) {
                val result = JsonHelper.convert(cacheResult.value, QueryResult::class.java)
                result.setQueryProperties(query, limit)
                result.setProperty("cacheTime", cacheResult.createdAt)
                val stats = QueryStats(QueryStats.State.FINISHED, query, nodes = 1, percentage = 100.0)
                return Task.completedTask(auth, result, stats)
            }
        }

        val queryWithComments = bridge.generateQuery(context.viewModels, rawSqlQuery, context.comments)
        val task = dataSource.createQueryTask(
            auth.warehouseAuth(),
            queryWithComments,
            queryOptions.defaultSchema,
            queryOptions.defaultDatabase,
            limit,
            isBackgroundTask
        )

        postProcessors.forEach { task.addPostProcessor(it) }

        runningTasks[identifierForTask] = task

        task.onFinish {
            if (it != null && it.error == null) {
                val cacheKey = ICacheService.CacheKey(auth.projectId, SQL_QUERY, cacheIdentifier)
                cacheService.setCache(
                    cacheKey,
                    it,
                    Duration.ofMinutes(60)
                )
            }

            MetriqlEventBus.publish(SQLExecuteEvent(auth, queryWithComments, info?.first, info?.second, task.getDuration().toMillis(), it?.error))
            runningTasks.remove(identifierForTask)
        }

        task.addPostProcessor {
            it.setQueryProperties(queryWithComments, limit)
            it
        }

        return task
    }
}
