package com.metriql.warehouse.mysql

import com.fasterxml.jackson.annotation.JsonAlias
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.spi.Warehouse

object MySQLWarehouse : Warehouse<MySQLWarehouse.MysqlConfig> {
    override val names = Warehouse.Name("mysql", null)
    override val configClass = MysqlConfig::class.java

    override fun getDataSource(config: MysqlConfig) = MySQLDataSource(config)

    override val bridge = MySQLMetriqlBridge

    data class MysqlConfig(
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

class MySQLWarehouseProxy : Warehouse<MySQLWarehouse.MysqlConfig> by MySQLWarehouse
