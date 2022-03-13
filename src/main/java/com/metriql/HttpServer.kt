package com.metriql

import com.google.common.cache.CacheBuilderSpec
import com.google.common.collect.ImmutableMap
import com.google.common.net.HostAndPort
import com.metriql.bootstrap.OptionMethodHttpService
import com.metriql.deployment.Deployment
import com.metriql.report.IAdHocService
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.report.SqlQueryTaskGenerator
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.funnel.FunnelReportType
import com.metriql.report.funnel.FunnelService
import com.metriql.report.mql.MqlReportType
import com.metriql.report.mql.MqlService
import com.metriql.report.retention.RetentionReportType
import com.metriql.report.retention.RetentionService
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.report.segmentation.SegmentationService
import com.metriql.report.sql.SqlReportType
import com.metriql.report.sql.SqlService
import com.metriql.service.QueryHttpService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttribute
import com.metriql.service.auth.UserAttributeDefinition
import com.metriql.service.auth.UserAttributeValues
import com.metriql.service.cache.InMemoryCacheService
import com.metriql.service.integration.IntegrationHttpService
import com.metriql.service.jdbc.NodeInfoService
import com.metriql.service.jdbc.QueryService
import com.metriql.service.jdbc.StatementService
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.IDatasetService
import com.metriql.service.model.ModelName
import com.metriql.service.suggestion.InMemorySuggestionCacheService
import com.metriql.service.suggestion.SuggestionCacheService
import com.metriql.service.suggestion.SuggestionService
import com.metriql.service.task.TaskExecutorService
import com.metriql.service.task.TaskHttpService
import com.metriql.service.task.TaskQueueService
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.logging.LogService
import com.metriql.warehouse.metriql.CatalogFile
import com.metriql.warehouse.spi.querycontext.DependencyFetcher
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.HttpResponseStatus
import io.swagger.util.PrimitiveType
import io.trino.sql.SqlToSegmentation
import org.rakam.server.http.HttpRequestException
import org.rakam.server.http.HttpServerBuilder
import org.rakam.server.http.HttpService
import java.time.ZoneId

object HttpServer {
    private val cacheConfig: CacheBuilderSpec = CacheBuilderSpec.parse("")

    private fun getServices(deployment: Deployment, enableJdbc: Boolean, catalogs: CatalogFile.Catalogs?): Pair<Set<HttpService>, () -> Unit> {
        val services = mutableSetOf<HttpService>()
        val postRun = mutableListOf<() -> Unit>()

        services.add(OptionMethodHttpService())
        services.add(BaseHttpService())
        services.add(IntegrationHttpService(deployment))

        val queryService = getQueryService(deployment)
        services.add(queryService)

        services.add(TaskHttpService(queryService.taskQueueService))

        if (enableJdbc) {
            val jdbcServices = jdbcServices(queryService, catalogs)
            jdbcServices.first.forEach { services.add(it) }
            postRun.add(jdbcServices.second)
        }

        return services to { postRun.forEach { it.invoke() } }
    }

    private fun getQueryService(deployment: Deployment): QueryHttpService {
        val taskExecutor = TaskExecutorService()
        val taskQueueService = TaskQueueService(taskExecutor)

        val cacheService = InMemoryCacheService(cacheConfig)
        val services = getReportServices(deployment.getModelService())

        val queryTaskGenerator = SqlQueryTaskGenerator(cacheService)
        val reportService = ReportService(
            deployment.getModelService(), JinjaRendererService(), queryTaskGenerator, services, this::getAttributes,
            object : DependencyFetcher {
                override fun fetch(context: IQueryGeneratorContext, model: ModelName): Recipe.Dependencies {
                    return Recipe.Dependencies()
                }
            }
        )

        val suggestionService = SuggestionService(
            InMemorySuggestionCacheService(cacheConfig),
            deployment,
            services[SegmentationReportType] as SegmentationService,
            reportService,
            queryTaskGenerator,
            taskQueueService
        )
        return QueryHttpService(deployment, reportService, taskQueueService, suggestionService, services)
    }

    private fun jdbcServices(queryService: QueryHttpService, catalogs: CatalogFile.Catalogs?): Pair<Set<HttpService>, () -> Unit> {
        val statementService = StatementService(queryService.taskQueueService, queryService.reportService, queryService.deployment)
        val services = setOf(
            NodeInfoService(),
            QueryService(queryService.taskQueueService),
            statementService,
        )
        return services to { statementService.startServices(catalogs) }
    }

    private fun getAttributes(auth: ProjectAuth): UserAttributeValues {
        return auth.attributes?.mapNotNull { item -> toAttribute(item.value)?.let { item.key to it } }?.toMap() ?: mapOf()
    }

    private fun toAttribute(value: Any?): UserAttribute? {
        return when (value) {
            is String -> UserAttribute(UserAttributeDefinition.Type.STRING, UserAttributeDefinition.Type.UserAttributeValue.StringValue(value))
            is Number -> UserAttribute(UserAttributeDefinition.Type.NUMERIC, UserAttributeDefinition.Type.UserAttributeValue.NumericValue(value))
            is Boolean -> UserAttribute(UserAttributeDefinition.Type.BOOLEAN, UserAttributeDefinition.Type.UserAttributeValue.BooleanValue(value))
            else -> null
        }
    }

    private fun getReportServices(modelService: IDatasetService): Map<ReportType, IAdHocService<out ServiceReportOptions>> {
        // we don't use a dependency injection system to speed up the initial start
        val segmentationService = SegmentationService()
        return mapOf(
            SegmentationReportType to segmentationService,
            FunnelReportType to FunnelService(modelService, segmentationService),
            RetentionReportType to RetentionService(modelService, segmentationService),
            SqlReportType to SqlService(),
            MqlReportType to MqlService(SqlToSegmentation(segmentationService, modelService)),
        )
    }

    fun start(
        address: HostAndPort,
        oauthApiSecret: String?,
        numberOfThreads: Int,
        isDebug: Boolean,
        origin: String?,
        deployment: Deployment,
        enableJdbc: Boolean,
        timezone: ZoneId?,
        catalogs: CatalogFile.Catalogs?
    ) {
        val eventExecutors: EventLoopGroup = if (Epoll.isAvailable()) {
            EpollEventLoopGroup(numberOfThreads)
        } else {
            NioEventLoopGroup(numberOfThreads)
        }

        val (services, postRun) = getServices(deployment, enableJdbc, catalogs)

        val httpServer = HttpServerBuilder()
            .setHttpServices(services)
            .setMaximumBody(104_857_600)
            .setEventLoopGroup(eventExecutors)
            .setMapper(JsonHelper.getMapper())
            .enableDebugMode(isDebug)
            .setExceptionHandler { request, ex ->
                if (ex is MetriqlException) {
                    LogService.logException(request, ex)
                }
                if (ex !is HttpRequestException) {
                    LogService.logException(request, ex)
                }
            }
            .setOverridenMappings(ImmutableMap.of(ZoneId::class.java, PrimitiveType.STRING))
            .setCustomRequestParameters(
                mapOf("userContext" to MetriqlAuthRequestParameterFactory(oauthApiSecret, deployment, timezone))
            )
            .addPostProcessor { response ->
                if (origin != null) {
                    response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true")
                    response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_HEADERS, "content-type,token")
                    response.headers().set("SameSite", "None; Secure")
                    if (!(response.headers().contains(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN))) {
                        response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, origin)
                    }
                } else {
                    response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                }
            }

        val build = httpServer.build()

        build.setNotFoundHandler {
            it.response("404", HttpResponseStatus.NOT_FOUND)
            if (origin != null) {
                it.addResponseHeader(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true")
                it.addResponseHeader(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, origin)
            }
            it.end()
        }

        build.bind(address.host, address.port)
        postRun.invoke()
        deployment.logStart()
    }
}
