package com.metriql

import com.google.common.cache.CacheBuilderSpec
import com.google.common.collect.ImmutableMap
import com.google.common.net.HostAndPort
import com.metriql.report.IAdHocService
import com.metriql.report.ReportType
import com.metriql.report.funnel.FunnelService
import com.metriql.report.retention.RetentionService
import com.metriql.report.segmentation.SegmentationService
import com.metriql.report.sql.SqlService
import com.metriql.service.cache.InMemoryCacheService
import com.metriql.service.model.IModelService
import com.metriql.service.task.TaskExecutorService
import com.metriql.service.task.TaskHttpService
import com.metriql.service.task.TaskQueueService
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.logging.LogService
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.jsonwebtoken.Claims
import io.jsonwebtoken.JwsHeader
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
import io.jsonwebtoken.SignatureException
import io.jsonwebtoken.SigningKeyResolver
import io.jsonwebtoken.UnsupportedJwtException
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.HttpResponseStatus
import io.swagger.util.PrimitiveType
import org.rakam.server.http.HttpRequestException
import org.rakam.server.http.HttpServerBuilder
import org.rakam.server.http.HttpService
import org.rakam.server.http.IRequestParameter
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.security.Key
import java.security.cert.CertificateFactory
import java.time.ZoneId
import java.util.Base64
import javax.crypto.spec.SecretKeySpec

object HttpServer {
    private fun getServices(modelService: IModelService, dataSource: DataSource, timezone: ZoneId?): Set<HttpService> {
        val taskExecutor = TaskExecutorService()
        val taskQueueService = TaskQueueService(taskExecutor)

        val cacheConfig = CacheBuilderSpec.parse("")
        val apiService = QueryHttpService(modelService, dataSource, InMemoryCacheService(cacheConfig), taskQueueService, getReportServices(modelService), timezone)
        return setOf(BaseHttpService(), TaskHttpService(taskQueueService), apiService)
    }

    private fun getReportServices(modelService: IModelService): Map<ReportType, IAdHocService<out ServiceReportOptions>> {
        // we don't use a dependency injection system to speed up the initial start
        val segmentationService = SegmentationService()
        return mapOf(
            ReportType.SEGMENTATION to segmentationService,
            ReportType.FUNNEL to FunnelService(modelService, segmentationService),
            ReportType.RETENTION to RetentionService(modelService, segmentationService),
            ReportType.SQL to SqlService()
        )
    }

    fun start(
        address: HostAndPort,
        apiSecret: String?,
        numberOfThreads: Int,
        isDebug: Boolean,
        origin: String?,
        modelService: IModelService,
        dataSource: DataSource,
        timezone: ZoneId?
    ) {
        val eventExecutors: EventLoopGroup = if (Epoll.isAvailable()) {
            EpollEventLoopGroup(numberOfThreads)
        } else {
            NioEventLoopGroup(numberOfThreads)
        }

        val httpServer = HttpServerBuilder()
            .setHttpServices(getServices(modelService, dataSource, timezone))
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
                mapOf(
                    "auth" to HttpServerBuilder.IRequestParameterFactory {
                        IRequestParameter { _, request ->
                            if (apiSecret == null) {
                                null
                            } else {
                                val token = request.headers().get(HttpHeaders.Names.AUTHORIZATION)?.split(" ".toRegex(), 2)
                                    ?: throw MetriqlException(HttpResponseStatus.UNAUTHORIZED)
                                if (token[0].lowercase() != "bearer") {
                                    throw MetriqlException("Only the `Bearer` Authorization is accepted", HttpResponseStatus.BAD_REQUEST)
                                }

                                val key = loadKeyFile(apiSecret)

                                val parser = Jwts.parser()
                                parser.setSigningKeyResolver(object : SigningKeyResolver {
                                    override fun resolveSigningKey(header: JwsHeader<out JwsHeader<*>>?, claims: Claims?): Key? {
                                        val algorithm = SignatureAlgorithm.forName(header!!.algorithm)
                                        return key.getKey(algorithm)
                                    }

                                    override fun resolveSigningKey(header: JwsHeader<out JwsHeader<*>>?, plaintext: String?): Key? {
                                        val algorithm = SignatureAlgorithm.forName(header!!.algorithm)
                                        return key.getKey(algorithm)
                                    }
                                })

                                try {
                                    parser.parse(token[1]).body
                                } catch (e: Exception) {
                                    throw MetriqlException(HttpResponseStatus.UNAUTHORIZED)
                                }
                            }
                        }
                    }
                )
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

    class LoadedKey(private val publicKey: Key?, private val hmacKey: ByteArray?) {
        fun getKey(algorithm: SignatureAlgorithm): Key? {
            return if (algorithm.isHmac) {
                if (hmacKey == null) {
                    throw UnsupportedJwtException(String.format("JWT is signed with %s, but no HMAC key is configured", algorithm))
                } else {
                    SecretKeySpec(hmacKey, algorithm.jcaName)
                }
            } else publicKey ?: throw UnsupportedJwtException(String.format("JWT is signed with %s, but no key is configured", algorithm))
        }
    }

    private fun loadKeyFile(value: String): LoadedKey {
        return try {
            val cf = CertificateFactory.getInstance("X.509")
            val cert = cf.generateCertificate(ByteArrayInputStream(value.toByteArray(StandardCharsets.UTF_8)))
            LoadedKey(cert.publicKey, null)
        } catch (var4: Exception) {
            try {
                val rawKey = Base64.getMimeDecoder().decode(value.toByteArray(StandardCharsets.US_ASCII))
                LoadedKey(null, rawKey)
            } catch (var3: IOException) {
                throw SignatureException("Unknown signing key id")
            }
        }
    }
}
