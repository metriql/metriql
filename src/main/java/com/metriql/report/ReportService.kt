package com.metriql.report

import com.metriql.db.QueryResult
import com.metriql.report.sql.SqlReportOptions
import com.metriql.service.audit.MetriqlEvents
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttributeFetcher
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.IModelService
import com.metriql.service.task.Task
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions

open class ReportService(
    protected val modelService: IModelService,
    protected val rendererService: JinjaRendererService,
    protected val queryTaskGenerator: SqlQueryTaskGenerator,
    val services: Map<ReportType, IAdHocService<out ServiceReportOptions>>,
    protected val userAttributeFetcher: UserAttributeFetcher,
) {
    fun getServiceForReportType(reportType: ReportType) = services.getValue(reportType) as IAdHocService<in ServiceReportOptions>

    fun createContext(auth: ProjectAuth, dataSource: DataSource): QueryGeneratorContext {
        return QueryGeneratorContext(
            auth,
            dataSource,
            modelService,
            rendererService,
            reportExecutor = object : ReportExecutor {
                override fun getQuery(auth: ProjectAuth, type: ReportType, options: RecipeQuery): String {
                    val context = createContext(auth, dataSource)
                    return getServiceForReportType(type).renderQuery(
                        auth,
                        context,
                        options.toReportOptions(context),
                        listOf(),
                    ).query
                }
            },
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
        val (query, postProcessors, sqlQueryOptions) = try {
            getServiceForReportType(reportType).renderQuery(
                auth,
                context,
                options,
                reportFilters,
            )
        } catch (e: Exception) {
            return Task.completedTask(auth, QueryResult.errorResult(QueryResult.QueryError.create(e)), QueryResult.QueryStats(QueryResult.QueryStats.State.FINISHED.name))
        }

        val queryOptions = sqlQueryOptions ?: SqlReportOptions.QueryOptions(options.getQueryLimit() ?: WarehouseQueryTask.DEFAULT_LIMIT, null, null, useCache)
        return queryTaskGenerator.createTask(
            auth,
            context,
            dataSource,
            query,
            queryOptions,
            isBackgroundTask,
            info = Pair(MetriqlEvents.AuditLog.SQLExecuteEvent.SQLContext.ADHOC_REPORT, MetriqlEvents.AuditLog.SQLExecuteEvent.SQLContext.AdhocReport(reportType, options)),
            postProcessors = postProcessors
        )
    }
}
