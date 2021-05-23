package com.metriql.cache

import com.fasterxml.jackson.databind.JsonNode
import com.metriql.util.JsonHelper
import com.metriql.util.UppercaseEnum
import java.time.Duration
import java.time.Instant

interface ICacheService {
    fun setCache(cacheKey: CacheKey, value: Any?, ttl: Duration? = null)
    fun getCache(cacheKey: CacheKey): CacheResult?

    fun invalidate(projectId: Int, entityType: EntityType, entityJsonPath: List<String>, value: String)
    fun invalidate(projectId: Int, entityType: EntityType)
    fun invalidate(projectId: Int)

    data class CacheKey(
        val projectId: Int,
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
}
