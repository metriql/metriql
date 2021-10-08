package com.metriql.warehouse.clickhouse

import com.fasterxml.jackson.annotation.JsonAlias
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.clickhouse.ClickhouseWarehouse.ClickhouseConfig
import com.metriql.warehouse.spi.Warehouse
import ru.yandex.clickhouse.ClickHouseDriver
import java.sql.DriverManager

object ClickhouseWarehouse : Warehouse<ClickhouseConfig> {
    init {
        DriverManager.registerDriver(ClickHouseDriver())
    }

    override val names = Warehouse.Name("clickhouse", "clickhouse")
    override val configClass = ClickhouseConfig::class.java

    override fun getDataSource(config: ClickhouseConfig) = ClickhouseDataSource(config)

    override val bridge = ClickhouseMetriqlBridge

    data class ClickhouseConfig(
        @JsonAlias("server")
        val host: String,
        val port: Int,
        val database: String,
        val user: String,
        @JsonAlias("pass")
        val password: String,
        val usePool: Boolean,
        val connectionParameters: Map<String, String>?
    ) : Warehouse.Config {
        override fun toString(): String = "$database - $host"
        override fun stripPassword() = this.copy(password = "")
        override fun isValid() = ValidationUtil.checkForPrivateIPAccess(host) == null
        override fun warehouseSchema() = null
        override fun warehouseDatabase() = database

        override fun withUsernamePassword(username: String, password: String): Warehouse.Config {
            return this.copy(user = username, password = password)
        }
    }
}

class ClickhouseWarehouseProxy : Warehouse<ClickhouseConfig> by ClickhouseWarehouse
