package com.metriql.service.suggestion

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheBuilderSpec
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.DimensionName
import com.metriql.service.model.ModelName
import com.metriql.util.JsonHelper
import java.time.Instant

open class InMemorySuggestionCacheService(spec: CacheBuilderSpec) : SuggestionCacheService {
    private val cache = CacheBuilder.from(spec)
        .weigher<CacheKey, CacheValue> { _, value -> value.calculateSize() }
        .build<CacheKey, CacheValue>()
    open fun get(auth: ProjectAuth, modelName: ModelName, dimensionName: DimensionName): CacheValue? {
        return cache.getIfPresent(CacheKey(auth.projectId, modelName, dimensionName))
    }

    override fun getCommon(auth: ProjectAuth, modelName: ModelName, dimensionName: DimensionName): SuggestionCacheService.Suggestion? {
        val items = get(auth, modelName, dimensionName) ?: return null
        return SuggestionCacheService.Suggestion(items.values.take(20), items.lastUpdated)
    }

    override fun search(auth: ProjectAuth, modelName: ModelName, dimensionName: DimensionName, filter: String): SuggestionCacheService.Suggestion? {
        val items = get(auth, modelName, dimensionName) ?: return null
        return SuggestionCacheService.Suggestion(items.values.filter { it.contains(filter, ignoreCase = true) }.take(20), items.lastUpdated)
    }

    override fun set(auth: ProjectAuth, modelName: ModelName, dimensionName: DimensionName, values: List<String>) {
        cache.put(CacheKey(auth.projectId, modelName, dimensionName), CacheValue(values, Instant.now()))
    }

    data class CacheKey(
        val projectId: String,
        val modelName: String,
        val dimension: String
    ) {
        override fun toString(): String {
            return JsonHelper.encode(this)
        }
    }

    data class CacheValue(
        val values: List<String>,
        val lastUpdated: Instant
    ) {
        fun calculateSize(): Int {
            return values.sumOf { it.length }
        }
    }
}
