package com.metriql.report

import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.QueryStats.State.FINISHED
import com.metriql.report.data.ReportFilter
import com.metriql.report.sql.SqlReportOptions
import com.metriql.service.audit.MetriqlEvents
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttributeFetcher
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.IModelService
import com.metriql.service.task.Task
import com.metriql.warehouse.WarehouseQueryTask.Companion.DEFAULT_LIMIT
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.DependencyFetcher
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceReportOptions
import java.util.UUID

open class ReportService(
    protected val modelService: IModelService,
    protected val rendererService: JinjaRendererService,
    protected val queryTaskGenerator: SqlQueryTaskGenerator,
    val services: Map<ReportType, IAdHocService<out ServiceReportOptions>>,
    private val userAttributeFetcher: UserAttributeFetcher,
    private val dependencyFetcher: DependencyFetcher,
) {
    fun getServiceForReportType(reportType: ReportType) = services.getValue(reportType) as IAdHocService<in ServiceReportOptions>

    fun createContext(auth: ProjectAuth, dataSource: DataSource): QueryGeneratorContext {
        return QueryGeneratorContext(
            auth,
            dataSource,
            modelService,
            rendererService,
            reportExecutor = { auth, type, options ->
                val context = createContext(auth, dataSource)
                getServiceForReportType(type).renderQuery(
                    auth,
                    context,
                    options.toReportOptions(context),
                    listOf(),
                ).query
            },
            dependencyFetcher = dependencyFetcher,
            userAttributeFetcher = { userAttributeFetcher.invoke(it) }
        )
    }

    fun <T : ServiceReportOptions> queryTask(
        auth: ProjectAuth,
        reportType: ReportType,
        dataSource: DataSource,
        options: T,
        reportFilters: List<ReportFilter> = listOf(),
        isBackgroundTask: Boolean = false,
        useCache: Boolean = true,
        variables: Map<String, Any>? = null,
        context: IQueryGeneratorContext = createContext(auth, dataSource)
    ): QueryTask {
        try {
            val (query, postProcessors, sqlQueryOptions) = getServiceForReportType(reportType).renderQuery(
                auth,
                context,
                options,
                reportFilters,
            )

            val queryOptions = sqlQueryOptions ?: SqlReportOptions.QueryOptions(options.getQueryLimit() ?: DEFAULT_LIMIT, null, null, useCache)
            return queryTaskGenerator.createTask(
                auth,
                context,
                dataSource,
                query,
                queryOptions,
                isBackgroundTask,
                info = MetriqlEvents.AuditLog.SQLExecuteEvent.SQLContext.AdhocReport(reportType, options),
                postProcessors = postProcessors
            )
        } catch (e: Throwable) {
            return Task.completedTask(auth, UUID.randomUUID(), QueryResult.errorResult(QueryResult.QueryError.create(e)), QueryResult.QueryStats(FINISHED, null), failed = true)
        }
    }
}
