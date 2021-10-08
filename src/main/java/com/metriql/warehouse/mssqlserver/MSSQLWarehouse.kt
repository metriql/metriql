package com.metriql.warehouse.mssqlserver

import com.fasterxml.jackson.annotation.JsonAlias
import com.metriql.util.ValidationUtil.checkForPrivateIPAccess
import com.metriql.warehouse.spi.Warehouse
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import java.sql.DriverManager

object MSSQLWarehouse : Warehouse<MSSQLWarehouse.MSSQLServerConfig> {

    init {
        DriverManager.registerDriver(SQLServerDriver())
    }

    override val names = Warehouse.Name("mssqlServer", "sqlserver")
    override val configClass = MSSQLServerConfig::class.java

    override fun getDataSource(config: MSSQLServerConfig) = MSSQLDataSource(config)

    override val bridge = MSSQLMetriqlBridge

    data class MSSQLServerConfig(
        @JsonAlias("server")
        val host: String,
        val port: Int,
        val database: String,
        val schema: String,
        val user: String,
        val password: String,
        val usePool: Boolean,
        val connectionParameters: Map<String, String>?
    ) : Warehouse.Config {
        override fun toString(): String = "$database - $host"
        override fun stripPassword() = this.copy(password = "")
        override fun isValid() = checkForPrivateIPAccess(host) == null
        override fun warehouseSchema() = schema
        override fun warehouseDatabase() = database

        override fun withUsernamePassword(username: String, password: String): Warehouse.Config {
            return this.copy(user = username, password = password)
        }
    }
}

class MSSQLWarehouseProxy : Warehouse<MSSQLWarehouse.MSSQLServerConfig> by MSSQLWarehouse
