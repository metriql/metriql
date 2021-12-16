package com.metriql.warehouse.spi

import com.fasterxml.jackson.annotation.JsonIgnore
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge

interface Warehouse<T : Warehouse.Config> {
    val bridge: WarehouseMetriqlBridge
    val names: Name
    val configClass: Class<in T>

    fun getDataSource(config: T): DataSource

    interface Config {
        @JsonIgnore
        fun stripPassword(): Config

        @JsonIgnore
        fun isValid(): Boolean

        @JsonIgnore
        fun warehouseSchema(): String?

        @JsonIgnore
        fun warehouseDatabase(): String?

        @JsonIgnore
        fun withUsernamePassword(username: String, password: String): Config {
            throw UnsupportedOperationException()
        }
    }

    data class Name(val metriql: String, val dbt: String?)
}
