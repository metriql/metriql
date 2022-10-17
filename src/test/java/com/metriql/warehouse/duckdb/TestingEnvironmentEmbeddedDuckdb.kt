package com.metriql.warehouse.duckdb

import com.metriql.tests.TestingServer
import java.sql.Connection

object TestingEnvironmentEmbeddedDuckdb : TestingServer<Connection> {
    override val config = DuckdbWarehouse.DuckdbConfig(
        null, // in-memory
    )

    override val dataSource = DuckdbDataSource(config, readOnly = false)
    override fun getQueryRunner() = dataSource.openConnection()
}
