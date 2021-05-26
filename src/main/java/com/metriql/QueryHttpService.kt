package com.metriql

import com.metriql.db.QueryResult
import com.metriql.report.IAdHocService
import com.metriql.report.Recipe
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.report.SqlQueryTaskGenerator
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttribute
import com.metriql.service.auth.UserAttributeDefinition.Type.BOOLEAN
import com.metriql.service.auth.UserAttributeDefinition.Type.NUMERIC
import com.metriql.service.auth.UserAttributeDefinition.Type.STRING
import com.metriql.service.auth.UserAttributeDefinition.Type.UserAttributeValue
import com.metriql.service.auth.UserAttributeValues
import com.metriql.service.cache.ICacheService
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.IModelService
import com.metriql.service.task.Task
import com.metriql.service.task.TaskQueueService
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.SuccessMessage
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions
import org.rakam.server.http.HttpService
import org.rakam.server.http.annotations.Api
import org.rakam.server.http.annotations.ApiOperation
import org.rakam.server.http.annotations.BodyParam
import org.rakam.server.http.annotations.JsonRequest
import java.time.ZoneId
import java.util.concurrent.CompletableFuture
import javax.inject.Named
import javax.ws.rs.GET
import javax.ws.rs.PUT
import javax.ws.rs.Path
import javax.ws.rs.QueryParam

@Api(value = CURRENT_VERSION, nickname = "query", description = "Query Service Endpoints", tags = ["query"])
@Path(CURRENT_VERSION)
class QueryHttpService(
    val modelService: IModelService,
    val dataSource: DataSource,
    cacheService: ICacheService,
    val taskQueueService: TaskQueueService,
    val services: Map<ReportType, IAdHocService<out ServiceReportOptions>>,
    val timezone: ZoneId?
) : HttpService() {
    val reportService = ReportService(modelService, JinjaRendererService(), SqlQueryTaskGenerator(cacheService), services, this::getAttributes)

    private fun getAttributes(auth: ProjectAuth): UserAttributeValues {
        val tempAuth = auth as? TempProjectAuth ?: throw IllegalStateException()
        return tempAuth.context?.mapNotNull { item -> toAttribute(item.value)?.let { item.key to it } }?.toMap() ?: mapOf()
    }

    private fun toAttribute(value: Any?): UserAttribute? {
        return when (value) {
            is String -> UserAttribute(STRING, UserAttributeValue.StringValue(value))
            is Number -> UserAttribute(NUMERIC, UserAttributeValue.NumericValue(value))
            is Boolean -> UserAttribute(BOOLEAN, UserAttributeValue.BooleanValue(value))
            else -> null
        }
    }

    @Path("/")
    @GET
    fun main(): String {
        return "See https://metriql.com/rest-api"
    }

    @ApiOperation(value = "Get metadata")
    @GET
    @Path("/metadata")
    fun metadata(@Named("auth") context: Map<String, Any?>?): List<Recipe.RecipeModel> {
        return modelService.list(TempProjectAuth(context, timezone)).map { Recipe.RecipeModel.fromModel(it) }
    }

    @ApiOperation(value = "Update manifest.json file")
    @PUT
    @Path("/update-manifest")
    fun updateManifest(): SuccessMessage {
        return null!!
    }

    @JsonRequest
    @Path("/sql")
    fun sql(@Named("auth") context: Map<String, Any?>?, @BodyParam query: Query): String {
        val auth = TempProjectAuth(context, timezone)
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
    fun query(
        @Named("auth") context: Map<String, Any?>?,
        @BodyParam query: Query,
        @QueryParam("useCache") useCache: Boolean?,
        @QueryParam("initialWaitInSeconds") initialWaitInSeconds: Long?
    ): CompletableFuture<Task.TaskTicket<QueryResult>> {
        val auth = TempProjectAuth(context, timezone)
        val context = reportService.createContext(auth, dataSource)
        val task = reportService.queryTask(
            auth, query.type, dataSource, query.report.toReportOptions(context),
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

    class TempProjectAuth(val context: Map<String, Any?>?, timezone: ZoneId?) : ProjectAuth(-1, -1, false, false, null, null, timezone)
}
