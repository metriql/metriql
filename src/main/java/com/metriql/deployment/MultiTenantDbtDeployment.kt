package com.metriql.deployment

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheBuilderSpec
import com.google.common.net.HttpHeaders
import com.metriql.Commands
import com.metriql.UserContext
import com.metriql.deployment.SingleTenantDbtDeployment.Companion.parseRecipe
import com.metriql.report.jinja.JinjaApps
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.DatasetName
import com.metriql.service.dataset.IDatasetService
import com.metriql.service.suggestion.InMemorySuggestionCacheService
import com.metriql.service.suggestion.SuggestionCacheService
import com.metriql.util.MetriqlException
import com.metriql.util.UnirestHelper
import com.metriql.warehouse.WarehouseConfig
import com.metriql.warehouse.WarehouseLocator
import com.metriql.warehouse.spi.DataSource
import io.airlift.units.Duration
import io.netty.handler.codec.http.HttpResponseStatus
import java.time.Instant
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

typealias ManifestCacheHolder = Optional<Pair<Instant, List<Dataset>>>

class MultiTenantDbtDeployment(private val multiTenantUrl: String, cacheExpiration: Duration, private val cacheSpec: CacheBuilderSpec) : Deployment {
    private val cache = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
        .build<String, Optional<AdapterManifest>>()
    private val manifestCache = ConcurrentHashMap<String, ManifestCacheHolder>()

    private val internalDatasetService = MultiTenantDatasetService()
    override fun getDatasetService() = internalDatasetService
    override val authType = Deployment.AuthType.USERNAME_PASS
    override fun getAuth(context: UserContext): ProjectAuth {
        val user = context.user ?: context.token ?: throw MetriqlException(HttpResponseStatus.UNAUTHORIZED)
        val cachedAuth = cache.get(user) { Optional.empty() }
        val auth = ProjectAuth(
            user, "", isOwner = true,
            isSuperuser = true, email = null, permissions = null,
            attributes = mapOf(), timezone = null, source = null
        )

        if (cachedAuth.isEmpty || cachedAuth.get().modelsUpdatedAt == null) {
            synchronized(cachedAuth) {
                if (cache.getIfPresent(user)?.isEmpty == true || cachedAuth.get().modelsUpdatedAt == null) {
                    val get = UnirestHelper.unirest.get(multiTenantUrl)
                    if (context.token != null) {
                        get.header(HttpHeaders.AUTHORIZATION, "Bearer ${context.token}")
                    }
                    if (context.pass != null) {
                        get.basicAuth(user, context.pass)
                    }

                    val request = get
                        .header("Content-Type", "application/json")
                        .asObject(Response::class.java)
                    if (request.status != 200) {
                        throw MetriqlException("Unable to load models, multi-tenant request failed (${request.status})", HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    }
                    if (request.parsingError.isPresent) {
                        throw MetriqlException(
                            "Unable to load models, multi-tenant request failed with parsing error (${request.parsingError.get().message})",
                            HttpResponseStatus.INTERNAL_SERVER_ERROR
                        )
                    }

                    val dataSource = WarehouseLocator.getDataSource(request.body.connection_parameters)
                    val manifestUrl = request.body.manifest.url
                    val updateAtAndModelPair = manifestCache.computeIfAbsent(manifestUrl) { Optional.empty() }

                    val models = if (!updateAtAndModelPair.isPresent || updateAtAndModelPair.get().first < request.body.manifest.updated_at) {
                        synchronized(updateAtAndModelPair) {

                            val recipe = parseRecipe(dataSource, manifestUrl)
                            val models = SingleTenantDbtDeployment.getPreparedModels(dataSource, auth, recipe)
                            request.body.manifest.updated_at?.let {
                                manifestCache[manifestUrl] = Optional.of(it to models)
                            }
                            models
                        }
                    } else {
                        updateAtAndModelPair.get().second
                    }

                    cache.put(user, Optional.of(AdapterManifest(dataSource, models, request.body.manifest.updated_at)))
                }
            }
        }

        return auth
    }

    override fun getCacheService(): SuggestionCacheService {
        return InMemorySuggestionCacheService(cacheSpec)
    }

    override fun getDataSource(auth: ProjectAuth) = cache.getIfPresent(auth.userId as String)!!.get().dataSource
    override fun getApps(auth: ProjectAuth) = JinjaApps(listOf())

    inner class MultiTenantDatasetService : IDatasetService {

        override fun list(auth: ProjectAuth, target: Dataset.Target?): List<Dataset> {
            return cache.getIfPresent(auth.userId as String)!!.get().datasets
        }

        override fun getDataset(auth: ProjectAuth, datasetName: DatasetName): Dataset? {
            val regex = datasetName.toRegex()
            return list(auth).find { regex.matches(it.name) }
        }

        override fun update(auth: ProjectAuth) {
            throw MetriqlException(HttpResponseStatus.NOT_IMPLEMENTED)
        }
    }

    override fun logStart() {
        Commands.logger.info("Started multi-tenant Metriql deployment")
    }

    data class AdapterManifest(val dataSource: DataSource, val datasets: List<Dataset>, val modelsUpdatedAt: Instant?)

    data class Response(val manifest: ManifestLocation, val connection_parameters: WarehouseConfig) {
        data class ManifestLocation(val url: String, val updated_at: Instant?)
    }
}
