package com.metriql.service.cache

import com.fasterxml.jackson.databind.JsonNode
import com.metriql.util.JsonHelper
import com.metriql.util.UppercaseEnum
import java.time.Duration
import java.time.Instant

interface ICacheService {
    fun setCache(cacheKey: CacheKey, value: CacheValue?, ttl: Duration? = null)
    fun getCache(cacheKey: CacheKey): CacheResult?
    fun invalidate(projectId: String, entityType: EntityType, entityJsonPath: List<String>, value: String)
    fun invalidate(projectId: String, entityType: EntityType)
    fun invalidate(projectId: String)

    data class CacheKey(
        val projectId: String,
        val entityType: EntityType,
        val options: Any?
    ) {
        override fun toString(): String {
            return JsonHelper.encode(this)
        }
    }

    @UppercaseEnum
    enum class EntityType {
        DASHBOARD_ITEM, SQL_QUERY
    }

    data class CacheResult(val createdAt: Instant, val value: JsonNode) {
        fun isExpired(ttlInSeconds: Long): Boolean = createdAt.plusSeconds(ttlInSeconds) < Instant.now()
    }

    interface CacheValue {
        fun calculateSize() : Int
    }
}
