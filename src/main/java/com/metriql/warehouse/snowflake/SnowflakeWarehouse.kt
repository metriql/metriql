package com.metriql.warehouse.snowflake

import com.fasterxml.jackson.annotation.JsonAlias
import com.metriql.warehouse.spi.Warehouse
import net.snowflake.client.jdbc.SnowflakeDriver
import java.sql.DriverManager

object SnowflakeWarehouse : Warehouse<SnowflakeWarehouse.SnowflakeConfig> {

    init {
        DriverManager.registerDriver(SnowflakeDriver())
    }

    override val names = setOf("snowflake")
    override val configClass = SnowflakeConfig::class.java
    override fun getDataSource(config: SnowflakeConfig) = SnowflakeDataSource(config)
    override val bridge = SnowflakeMetriqlBridge

    data class SnowflakeConfig(
        val account: String,
        val user: String,
        @JsonAlias("pass")
        val password: String?,
        val database: String,
        val schema: String,
        val regionId: String?,
        val warehouse: String?,
        val private_key_path: String?,
        val private_key_passphrase: String?,
        val role: String? = null,
        val usePool: Boolean? = null,
        val query_tag: String? = null,
        val connectionParameters: Map<String, String>? = null,
        val client_session_keep_alive: Boolean? = null,
    ) : Warehouse.Config {
        override fun toString(): String = "$database - $schema"
        override fun stripPassword() = this.copy(password = "")
        override fun isValid() = true
        override fun warehouseSchema() = schema
        override fun warehouseDatabase() = database

        override fun withUsernamePassword(username: String, password: String): Warehouse.Config {
            return this.copy(user = username, password = password)
        }
    }
}

class SnowflakeWarehouseProxy : Warehouse<SnowflakeWarehouse.SnowflakeConfig> by SnowflakeWarehouse
