package com.metriql.service

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.metriql.CURRENT_PATH
import com.metriql.db.QueryResult
import com.metriql.deployment.Deployment
import com.metriql.report.IAdHocService
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.Model
import com.metriql.service.suggestion.SuggestionService
import com.metriql.service.task.Task
import com.metriql.service.task.TaskQueueService
import com.metriql.util.MetriqlException
import com.metriql.util.SuccessMessage
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.netty.handler.codec.http.HttpResponseStatus
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
    val deployment: Deployment,
    val reportService: ReportService,
    val taskQueueService: TaskQueueService,
    val suggestionService: SuggestionService,
    val services: Map<ReportType, IAdHocService<out ServiceReportOptions>>
) : HttpService() {
    private val startTime: Instant = Instant.now()
    private var lastMetadataChangeTime: Instant = Instant.now()

    @ApiOperation(value = "Get metadata")
    @GET
    @Path("/metadata")
    fun metadata(@Named("userContext") auth: ProjectAuth): List<Model> {
        return deployment.getModelService().list(auth)
    }

    @ApiOperation(value = "Get datasets")
    @GET
    @Path("/metadata/datasets")
    fun metadataDatasetNames(@Named("userContext") auth: ProjectAuth): Set<String> {
        return deployment.getModelService().listDatasetNames(auth)
    }

    @ApiOperation(value = "Get datasets")
    @GET
    @Path("/suggest")
    fun suggest(@Named("userContext") auth: ProjectAuth,  @BodyParam query: SuggestionService.SuggestionQuery,): CompletableFuture<List<String>> {
        return suggestionService.search(auth, query.value, query.filter)
    }

    @ApiOperation(value = "Get datasets")
    @GET
    @Path("/metadata/dataset")
    fun metadataDataset(@Named("userContext") auth: ProjectAuth, @QueryParam("name") name: String): Model {
        return deployment.getModelService().getDataset(auth, name) ?: throw MetriqlException(HttpResponseStatus.NOT_FOUND)
    }

    @ApiOperation(value = "Get ticker info")
    @GET
    @Path("/ticker")
    fun ticker(): TickerInfo {
        val activeTasks = taskQueueService.currentTasks().count { !it.status.isDone }
        return TickerInfo(activeTasks, lastMetadataChangeTime, startTime)
    }

    @ApiOperation(value = "Update manifest.json file")
    @PUT
    @Path("/update-manifest")
    fun updateManifest(@Named("userContext") auth: ProjectAuth): SuccessMessage {
        deployment.getModelService().update(auth)
        synchronized(this) {
            lastMetadataChangeTime = Instant.now()
        }
        return SuccessMessage.success()
    }

    @ApiOperation(value = "Generate SQL")
    @JsonRequest
    @Path("/sql")
    fun sql(@Named("userContext") auth: ProjectAuth, @BodyParam query: Query): String {
        val context = reportService.createContext(auth, deployment.getDataSource(auth))
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
        @QueryParam("useCache", required = false) useCache: Boolean?,
        @QueryParam("initialWaitInSeconds", required = false) initialWaitInSeconds: Long?
    ): CompletableFuture<Task.TaskTicket<QueryResult>> {
        val context = reportService.createContext(auth, deployment.getDataSource(auth))
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
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
        val report: RecipeQuery
    )

    data class TickerInfo(val activeTasks: Int, val lastMetadataChangeTime: Instant, val startTime: Instant)
}
