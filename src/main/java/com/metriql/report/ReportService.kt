package com.metriql.report

import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.QueryStats.State.FINISHED
import com.metriql.report.data.FilterValue
import com.metriql.report.sql.SqlQuery
import com.metriql.service.audit.MetriqlEvents
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttributeFetcher
import com.metriql.service.dataset.IDatasetService
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.task.Task
import com.metriql.util.MetriqlException
import com.metriql.warehouse.WarehouseQueryTask.Companion.DEFAULT_LIMIT
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.DependencyFetcher
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceQuery
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.UUID

class ReportService(
    private val datasetService: IDatasetService,
    private val rendererService: JinjaRendererService,
    private val queryTaskGenerators: List<QueryTaskGenerator>,
    val services: Map<ReportType, IAdHocService<out ServiceQuery>>,
    private val userAttributeFetcher: UserAttributeFetcher,
    private val dependencyFetcher: DependencyFetcher,
) {
    fun getServiceForReportType(reportType: ReportType) = services.getValue(reportType) as IAdHocService<in ServiceQuery>

    fun createContext(auth: ProjectAuth, dataSource: DataSource): QueryGeneratorContext {
        return QueryGeneratorContext(
            auth,
            dataSource,
            datasetService,
            rendererService,
            reportExecutor = { auth, type, options ->
                val context = createContext(auth, dataSource)
                getServiceForReportType(type).renderQuery(
                    auth,
                    context,
                    options.toReportOptions(context),
                ).query
            },
            dependencyFetcher = dependencyFetcher,
            userAttributeFetcher = { userAttributeFetcher.invoke(it) },
        )
    }

    fun <T : ServiceQuery> queryTask(
        auth: ProjectAuth,
        reportType: ReportType,
        dataSource: DataSource,
        options: T,
        reportFilters: FilterValue? = null,
        isBackgroundTask: Boolean = false,
        useCache: Boolean = true,
        context: IQueryGeneratorContext = createContext(auth, dataSource)
    ): QueryTask {
        try {
            val (query, postProcessors, sqlQueryOptions, target) = getServiceForReportType(reportType).renderQuery(
                auth,
                context,
                options,
                reportFilters,
            )

            val queryTaskGenerator = (
                queryTaskGenerators.find { it.javaClass == target.java }
                    ?: throw MetriqlException("Unable to find task generator $target", HttpResponseStatus.INTERNAL_SERVER_ERROR)
                )

            val queryOptions = sqlQueryOptions ?: SqlQuery.QueryOptions(DEFAULT_LIMIT, null, null, useCache)
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
