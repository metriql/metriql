package com.metriql.warehouse.duckdb

import com.metriql.warehouse.spi.Warehouse
import java.sql.DriverManager

object DuckdbWarehouse : Warehouse<DuckdbWarehouse.DuckdbConfig> {
    init {
        DriverManager.registerDriver(org.duckdb.DuckDBDriver())
    }

    override val names = setOf("duckdb")
    override val configClass = DuckdbConfig::class.java

    override fun getDataSource(config: DuckdbConfig) = DuckdbDataSource(config)

    override val bridge = DuckdbMetriqlBridge

    data class DuckdbConfig(
        val db_path: String?,
        val connectionParameters: Map<String, String>? = null,
    ) : Warehouse.Config {
        override fun toString(): String = db_path ?: ":memory:"
        override fun stripPassword() = this
        override fun warehouseSchema() = null
        override fun warehouseDatabase() = db_path
    }
}

class DuckdbWarehouseProxy : Warehouse<DuckdbWarehouse.DuckdbConfig> by DuckdbWarehouse
