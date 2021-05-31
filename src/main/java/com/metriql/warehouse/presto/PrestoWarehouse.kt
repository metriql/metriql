package com.metriql.warehouse.presto

import com.metriql.util.ValidationUtil
import com.metriql.warehouse.spi.Warehouse
import io.trino.jdbc.TrinoDriver
import java.sql.DriverManager

object PrestoWarehouse : Warehouse<PrestoWarehouse.PrestoConfig> {
    init {
        DriverManager.registerDriver(TrinoDriver())
    }

    override val names = Warehouse.Name("presto", "presto")
    override val configClass = PrestoConfig::class.java
    override fun getDataSource(config: PrestoConfig) = PrestoDataSource(config)

    override val bridge = PrestoMetriqlBridge

    data class PrestoConfig(
        val host: String,
        val port: Int,
        val catalog: String,
        val schema: String,
        val user: String,
        val password: String?,
        val method: Method? = null,
        val usePool: Boolean? = null,
        val connectionParameters: Map<String, String>? = null
    ) : Warehouse.Config {
        enum class Method { none, ldap, kerberos }

        override fun toString(): String = "$catalog - $schema"
        override fun stripPassword() = this.copy(password = "")
        override fun isValid() = ValidationUtil.checkForPrivateIPAccess(host) == null
        override fun warehouseSchema() = schema
        override fun warehouseDatabase() = catalog
    }
}

class PrestoWarehouseProxy : Warehouse<PrestoWarehouse.PrestoConfig> by PrestoWarehouse
