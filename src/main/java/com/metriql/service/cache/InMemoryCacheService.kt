package com.metriql.service.cache

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheBuilderSpec
import com.metriql.util.JsonHelper
import java.time.Duration
import java.time.Instant

class InMemoryCacheService(spec: CacheBuilderSpec) : ICacheService {
    private val cache = CacheBuilder.from(spec).build<ICacheService.CacheKey, InMemoryCacheItem>()

    override fun setCache(cacheKey: ICacheService.CacheKey, value: Any?, ttl: Duration?) {
        cache.put(cacheKey, InMemoryCacheItem(ttl, Instant.now(), value))
    }

    override fun getCache(cacheKey: ICacheService.CacheKey): ICacheService.CacheResult? {
        val value = cache.getIfPresent(cacheKey) ?: return null
        return ICacheService.CacheResult(value.createdAt, JsonHelper.convert(value.value, ObjectNode::class.java))
    }

    private fun lookup(options: Any?, entityJsonPath: List<String>, value: String, currentIndex: Int = 0): Boolean {
        if (options == null) {
            return false
        }

        val map = if (options is Map<*, *>) {
            options
        } else {
            // TODO: work on the performance improvements
            JsonHelper.convert(options, object : TypeReference<Map<String, Any>>() {})
        }

        val currentKey = map[entityJsonPath[currentIndex]]
        val nextIndex = currentIndex + 1

        return if (nextIndex >= currentIndex) {
            currentKey == value
        } else {
            lookup(currentKey, entityJsonPath, value, nextIndex)
        }
    }

    override fun invalidate(projectId: Int, entityType: ICacheService.EntityType, entityJsonPath: List<String>, value: String) {
        val keys = cache.asMap().keys.filter { it.projectId == projectId && it.entityType == entityType && lookup(it.options, entityJsonPath, value) }
        cache.invalidateAll()
    }

    override fun invalidate(projectId: Int, entityType: ICacheService.EntityType) {
        val keys = cache.asMap().keys.filter { it.projectId == projectId && it.entityType == entityType }
        cache.invalidateAll(keys)
    }

    override fun invalidate(projectId: Int) {
        val keys = cache.asMap().keys.filter { it.projectId == projectId }
        cache.invalidateAll(keys)
    }

    data class InMemoryCacheItem(val ttl: Duration?, val createdAt: Instant, val value: Any?)
}
