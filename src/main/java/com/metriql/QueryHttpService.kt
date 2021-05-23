package com.metriql

import com.metriql.auth.ProjectAuth
import com.metriql.cache.ICacheService
import com.metriql.db.QueryResult
import com.metriql.jinja.JinjaRendererService
import com.metriql.model.IModelService
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.report.SqlQueryTaskGenerator
import com.metriql.task.Task
import com.metriql.task.TaskQueueService
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.SuccessMessage
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.services.RecipeQuery
import org.rakam.server.http.HttpService
import org.rakam.server.http.annotations.Api
import org.rakam.server.http.annotations.ApiOperation
import org.rakam.server.http.annotations.BodyParam
import org.rakam.server.http.annotations.JsonRequest
import java.util.concurrent.CompletableFuture
import javax.inject.Named
import javax.ws.rs.GET
import javax.ws.rs.PUT
import javax.ws.rs.Path

@Api(value = CURRENT_VERSION, nickname = "query", description = "Query Service Endpoints", tags = ["query"])
@Path(CURRENT_VERSION)
class QueryHttpService(val modelService: IModelService, val dataSource: DataSource, cacheService: ICacheService, val taskQueueService: TaskQueueService) : HttpService() {
    val reportService = ReportService(modelService, JinjaRendererService(), SqlQueryTaskGenerator(cacheService)) {
        mapOf()
    }

    @Path("/")
    @GET
    fun main(): String {
        return "See https://metriql.com/rest-api"
    }

    @ApiOperation(value = "Get metadata")
    @GET
    @Path("/metadata")
    fun metadata(@Named("auth") context: Map<String, Any?>): List<Recipe.RecipeModel> {
        return modelService.list(ProjectAuth.singleProject()).map { Recipe.RecipeModel.fromModel(it) }
    }

    @ApiOperation(value = "Update manifest.json file")
    @PUT
    @Path("/update-manifest")
    fun updateManifest(): SuccessMessage {
        return null!!
    }

    @JsonRequest
    @Path("/sql")
    fun sql(@Named("auth") context: Map<String, Any?>, @BodyParam query: Query): String {
        val auth = ProjectAuth.singleProject()

        val context = reportService.createContext(auth, dataSource)
        val compiled = reportService.getServiceForReportType(query.type).renderQuery(
            auth,
            context,
            query.report.toReportOptions(context),
            listOf(),
        )

        return compiled.query
    }

    @JsonRequest
    @Path("/query")
    fun query(@Named("auth") context: Map<String, Any?>, @BodyParam query: Query): CompletableFuture<Task.TaskTicket<QueryResult>> {
        val auth = ProjectAuth.singleProject()

        val context = reportService.createContext(auth, dataSource)
        val task = reportService.queryTask(
            auth, query.type, dataSource, query.report.toReportOptions(context),
            isBackgroundTask = false,
            useCache = query.useCache ?: true,
            context = context
        )

        return taskQueueService.execute(task, query.initialWaitInSeconds ?: 60).thenApply { it.taskTicket() }
    }

    data class Query(
        val type: ReportType,
        @PolymorphicTypeStr<ReportType>(externalProperty = "type", valuesEnum = ReportType::class, name = "recipe")
        val report: RecipeQuery,
        val useCache: Boolean?,
        val initialWaitInSeconds: Long?
    )
}
