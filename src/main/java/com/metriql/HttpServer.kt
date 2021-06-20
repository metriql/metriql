package com.metriql

import com.google.common.cache.CacheBuilderSpec
import com.google.common.collect.ImmutableMap
import com.google.common.net.HostAndPort
import com.metriql.report.IAdHocService
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.report.SqlQueryTaskGenerator
import com.metriql.report.funnel.FunnelService
import com.metriql.report.retention.RetentionService
import com.metriql.report.segmentation.SegmentationService
import com.metriql.report.sql.MqlService
import com.metriql.report.sql.SqlService
import com.metriql.service.QueryHttpService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttribute
import com.metriql.service.auth.UserAttributeDefinition
import com.metriql.service.auth.UserAttributeValues
import com.metriql.service.cache.InMemoryCacheService
import com.metriql.service.jdbc.NodeInfoService
import com.metriql.service.jdbc.QueryService
import com.metriql.service.jdbc.StatementService
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.IModelService
import com.metriql.service.model.RecipeModelService
import com.metriql.service.task.TaskExecutorService
import com.metriql.service.task.TaskHttpService
import com.metriql.service.task.TaskQueueService
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.logging.LogService
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED
import io.swagger.util.PrimitiveType
import io.trino.sql.SqlToSegmentation
import org.rakam.server.http.HttpRequestException
import org.rakam.server.http.HttpServerBuilder
import org.rakam.server.http.HttpService
import java.time.ZoneId

object HttpServer {
    private fun getServices(modelService: RecipeModelService, dataSource: DataSource, enableJdbc: Boolean, timezone: ZoneId?): MutableSet<HttpService> {
        val services = mutableSetOf<HttpService>()

        services.add(BaseHttpService())

        val queryService = getQueryService(modelService, dataSource, timezone)
        services.add(queryService)

        services.add(TaskHttpService(queryService.taskQueueService))

        if (enableJdbc) {
            jdbcServices(queryService).forEach { services.add(it) }
        }

        return services
    }

    private fun getQueryService(modelService: RecipeModelService, dataSource: DataSource, timezone: ZoneId?): QueryHttpService {
        val taskExecutor = TaskExecutorService()
        val taskQueueService = TaskQueueService(taskExecutor)

        val cacheConfig = CacheBuilderSpec.parse("")
        val cacheService = InMemoryCacheService(cacheConfig)
        val services = getReportServices(modelService)

        val reportService = ReportService(modelService, JinjaRendererService(), SqlQueryTaskGenerator(cacheService), services, this::getAttributes)
        return QueryHttpService(modelService, { dataSource }, reportService, taskQueueService, services)
    }

    private fun jdbcServices(queryService: QueryHttpService): Set<HttpService> {
        return setOf(
            NodeInfoService(),
            StatementService(queryService.taskQueueService, queryService.reportService, queryService.dataSourceFetcher, queryService.modelService),
            QueryService(queryService.taskQueueService),
        )
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

    private fun getReportServices(modelService: IModelService): Map<ReportType, IAdHocService<out ServiceReportOptions>> {
        // we don't use a dependency injection system to speed up the initial start
        val segmentationService = SegmentationService()
        return mapOf(
            ReportType.SEGMENTATION to segmentationService,
            ReportType.FUNNEL to FunnelService(modelService, segmentationService),
            ReportType.RETENTION to RetentionService(modelService, segmentationService),
            ReportType.SQL to SqlService(),
            ReportType.MQL to MqlService(modelService, SqlToSegmentation(segmentationService, modelService)),
        )
    }

    fun start(
        address: HostAndPort,
        apiSecret: String?,
        usernamePass: String?,
        numberOfThreads: Int,
        isDebug: Boolean,
        origin: String?,
        modelService: RecipeModelService,
        dataSource: DataSource,
        enableJdbc: Boolean,
        timezone: ZoneId?
    ) {
        val eventExecutors: EventLoopGroup = if (Epoll.isAvailable()) {
            EpollEventLoopGroup(numberOfThreads)
        } else {
            NioEventLoopGroup(numberOfThreads)
        }

        val basicAuthLoader: BasicAuthLoader? = if (usernamePass != null) {
            val arr = usernamePass.split(":".toRegex(), 2)
            val (user, pass) = Pair(arr[0], arr[1]);
            {
                if (it.user == user && it.pass == pass) {
                    ProjectAuth(-1, -1, false, false, null, null, mapOf(), timezone)
                } else {
                    throw MetriqlException(UNAUTHORIZED)
                }
            }
        } else null

        val httpServer = HttpServerBuilder()
            .setHttpServices(getServices(modelService, dataSource, enableJdbc, timezone))
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
                mapOf("userContext" to MetriqlAuthRequestParameterFactory(apiSecret, basicAuthLoader))
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
    }
}
