package com.metriql.service

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.metriql.CURRENT_PATH
import com.metriql.db.QueryResult
import com.metriql.deployment.Deployment
import com.metriql.report.IAdHocService
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.report.segmentation.SegmentationQuery
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.IDatasetService
import com.metriql.service.suggestion.SuggestionService
import com.metriql.service.task.Task
import com.metriql.service.task.TaskQueueService
import com.metriql.util.MetriqlException
import com.metriql.util.SuccessMessage
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceQuery
import io.netty.handler.codec.http.HttpResponseStatus
import org.rakam.server.http.HttpService
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
    private val suggestionService: SuggestionService,
    val services: Map<ReportType, IAdHocService<out ServiceQuery>>
) : HttpService() {
    private val startTime: Instant = Instant.now()
    private var lastMetadataChangeTime: Instant = Instant.now()

    @ApiOperation(value = "Get metadata")
    @GET
    @Path("/metadata")
    fun metadata(@Named("userContext") auth: ProjectAuth): List<Dataset> {
        return deployment.getDatasetService().list(auth)
    }

    @ApiOperation(value = "Get datasets")
    @GET
    @Path("/metadata/datasets")
    fun metadataDatasetNames(@Named("userContext") auth: ProjectAuth): List<IDatasetService.DatasetLabel> {
        return deployment.getDatasetService().listDatasetNames(auth)
    }

    @ApiOperation(value = "Get datasets")
    @JsonRequest
    @Path("/metadata/search_values")
    fun suggest(@Named("userContext") auth: ProjectAuth, @BodyParam query: SuggestionService.SuggestionQuery): CompletableFuture<List<String>> {
        return suggestionService.search(auth, query.value, query.filter)
    }

    @ApiOperation(value = "Get datasets")
    @GET
    @Path("/metadata/dataset")
    fun metadataDataset(@Named("userContext") auth: ProjectAuth, @QueryParam("name") name: String): Dataset {
        return deployment.getDatasetService().getDataset(auth, name) ?: throw MetriqlException(HttpResponseStatus.NOT_FOUND)
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
        deployment.getDatasetService().update(auth)
        synchronized(this) {
            lastMetadataChangeTime = Instant.now()
        }
        return SuccessMessage.success()
    }

    @Deprecated("Use /compile")
    @ApiOperation(value = "Generate SQL")
    @JsonRequest
    @Path("/sql")
    fun sql(@Named("userContext") auth: ProjectAuth, @BodyParam query: InternalQuery): String {
        val context = reportService.createContext(auth, deployment.getDataSource(auth))
        val compiled = reportService.getServiceForReportType(query.type).renderQuery(
            auth,
            context,
            query.report,
        )

        return compiled.query
    }

    @Deprecated("Use /execute")
    @ApiOperation(value = "Run metriql internal queries")
    @JsonRequest
    @Path("/query")
    fun query(
        @Named("userContext") auth: ProjectAuth,
        @BodyParam query: InternalQuery,
        @QueryParam("useCache", required = false) useCache: Boolean?,
        @QueryParam("initialWaitInSeconds", required = false) initialWaitInSeconds: Long?
    ): CompletableFuture<Task.TaskTicket<QueryResult>> {
        val context = reportService.createContext(auth, deployment.getDataSource(auth))
        val task = reportService.queryTask(
            auth, query.type, context.datasource, query.report,
            isBackgroundTask = false,
            useCache = useCache ?: true,
            context = context
        )

        return taskQueueService.execute(task, initialWaitInSeconds ?: 60).thenApply { it.taskTicket() }
    }

    @ApiOperation(value = "Compile SQL")
    @JsonRequest
    @Path("/compile/segmentation")
    fun compile(@Named("userContext") auth: ProjectAuth, @BodyParam query: SegmentationQuery): String {
        val context = reportService.createContext(auth, deployment.getDataSource(auth))
        val compiled = reportService.getServiceForReportType(SegmentationReportType).renderQuery(
            auth,
            context,
            query,
        )

        return compiled.query
    }

    @ApiOperation(value = "Run metriql internal queries")
    @JsonRequest
    @Path("/execute/segmentation")
    fun execute(
        @Named("userContext") auth: ProjectAuth,
        @BodyParam query: SegmentationQuery,
        @QueryParam("useCache", required = false) useCache: Boolean?,
        @QueryParam("initialWaitInSeconds", required = false) initialWaitInSeconds: Long?
    ): CompletableFuture<Task.TaskTicket<QueryResult>> {
        val context = reportService.createContext(auth, deployment.getDataSource(auth))
        val task = reportService.queryTask(
            auth, SegmentationReportType, context.datasource, query,
            isBackgroundTask = false,
            useCache = useCache ?: true,
            context = context
        )

        return taskQueueService.execute(task, initialWaitInSeconds ?: 60).thenApply { it.taskTicket() }
    }

    data class InternalQuery(
        val type: ReportType,
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
        val report: ServiceQuery
    )

    data class Query(
        val type: ReportType,
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
        val report: RecipeQuery
    )

    data class TickerInfo(val activeTasks: Int, val lastMetadataChangeTime: Instant, val startTime: Instant)
}
