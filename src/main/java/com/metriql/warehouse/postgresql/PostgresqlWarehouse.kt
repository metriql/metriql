package com.metriql.warehouse.postgresql

import com.fasterxml.jackson.annotation.JsonAlias
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.postgresql.PostgresqlDataSource.Companion.DEFAULT_SCHEMA
import com.metriql.warehouse.spi.Warehouse
import java.sql.DriverManager

object PostgresqlWarehouse : Warehouse<PostgresqlWarehouse.PostgresqlConfig> {
    init {
        DriverManager.registerDriver(org.postgresql.Driver())
    }

    override val names = Warehouse.Name("postgresql", "postgres")
    override val configClass = PostgresqlConfig::class.java

    override fun getDataSource(config: PostgresqlConfig) = PostgresqlDataSource(config)

    override val bridge = PostgresqlMetriqlBridge

    data class PostgresqlConfig(
        val host: String,
        val port: Int,
        @JsonAlias("database")
        val dbname: String,
        val schema: String?,
        val user: String,
        @JsonAlias("pass")
        val password: String?,
        val method: Method? = null,
        val role: String? = null,
        val sslmode: String? = null,
        val usePool: Boolean? = null,
        val keepalives_idle: Int? = null,
        val connectionParameters: Map<String, String>? = null,
    ) : Warehouse.Config {
        enum class Method { iam }

        override fun toString(): String = "$dbname - ${schema ?: DEFAULT_SCHEMA})"
        override fun stripPassword() = this.copy(password = "")
        override fun isValid() = ValidationUtil.checkForPrivateIPAccess(host) == null
        override fun warehouseSchema() = schema
        override fun warehouseDatabase() = dbname

        override fun withUsernamePassword(username: String, password: String): Warehouse.Config {
            return this.copy(user = username, password = password)
        }
    }
}

class PostgresqlWarehouseProxy : Warehouse<PostgresqlWarehouse.PostgresqlConfig> by PostgresqlWarehouse
