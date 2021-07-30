package com.metriql.service

import com.metriql.CURRENT_PATH
import com.metriql.db.QueryResult
import com.metriql.report.IAdHocService
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.service.model.Model
import com.metriql.service.task.Task
import com.metriql.service.task.TaskQueueService
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.SuccessMessage
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import org.rakam.server.http.annotations.Api
import org.rakam.server.http.annotations.ApiOperation
import org.rakam.server.http.annotations.BodyParam
import org.rakam.server.http.annotations.JsonRequest
import org.rakam.server.http.annotations.QueryParam
import java.time.Instant
import java.util.concurrent.CompletableFuture
import javax.inject.Named
import javax.ws.rs.GET
import javax.ws.rs.PUT
import javax.ws.rs.Path

@Api(value = CURRENT_PATH, nickname = "query", description = "Query Service Endpoints", tags = ["query"])
@Path(CURRENT_PATH)
open class QueryHttpService(
    val modelService: IModelService,
    val dataSourceFetcher: (RakamHttpRequest) -> DataSource,
    val reportService: ReportService,
    val taskQueueService: TaskQueueService,
    val services: Map<ReportType, IAdHocService<out ServiceReportOptions>>
) : HttpService() {
    private var startTime : Instant = Instant.now()

    @ApiOperation(value = "Get metadata")
    @GET
    @Path("/metadata")
    fun metadata(@Named("userContext") auth: ProjectAuth): List<Model> {
        return modelService.list(auth)
    }

    fun getLastUpdateOnMetadata(): Instant {
        return startTime
    }

    @ApiOperation(value = "Update manifest.json file")
    @PUT
    @Path("/update-manifest")
    fun updateManifest(@Named("userContext") auth: ProjectAuth): SuccessMessage {
        modelService.update()
        synchronized(this) {
            startTime = Instant.now()
        }
        return SuccessMessage.success()
    }

    @ApiOperation(value = "Generate SQL")
    @JsonRequest
    @Path("/sql")
    fun sql(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, @BodyParam query: Query): String {
        val context = reportService.createContext(auth, dataSourceFetcher.invoke(request))
        val compiled = reportService.getServiceForReportType(query.type).renderQuery(
            auth,
            context,
            query.report.toReportOptions(context),
            listOf(),
        )

        return compiled.query
    }

    @ApiOperation(value = "Run metriql queries")
    @JsonRequest
    @Path("/query")
    fun query(
        request: RakamHttpRequest,
        @Named("userContext") auth: ProjectAuth,
        @BodyParam query: Query,
        @QueryParam("useCache") useCache: Boolean?,
        @QueryParam("initialWaitInSeconds") initialWaitInSeconds: Long?
    ): CompletableFuture<Task.TaskTicket<QueryResult>> {
        val context = reportService.createContext(auth, dataSourceFetcher.invoke(request))
        val task = reportService.queryTask(
            auth, query.type, context.datasource, query.report.toReportOptions(context),
            isBackgroundTask = false,
            useCache = useCache ?: true,
            context = context
        )

        return taskQueueService.execute(task, initialWaitInSeconds ?: 60).thenApply { it.taskTicket() }
    }

    data class Query(
        val type: ReportType,
        @PolymorphicTypeStr<ReportType>(externalProperty = "type", valuesEnum = ReportType::class, isNamed = true, name = "recipe")
        val report: RecipeQuery
    )
}
