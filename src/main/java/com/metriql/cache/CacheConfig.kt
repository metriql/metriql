package com.metriql.cache

import io.airlift.configuration.Config

class CacheConfig {
    private var type = Type.POSTGRES

    fun getType(): Type {
        return type
    }

    @Config("cache.type")
    fun setType(type: Type): CacheConfig {
        this.type = type
        return this
    }

    enum class Type {
        POSTGRES, IN_MEMORY
    }
}
