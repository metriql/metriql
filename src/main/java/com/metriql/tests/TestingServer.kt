package com.metriql.tests

import com.metriql.warehouse.spi.Warehouse
import java.sql.ResultSet

abstract class TestingServer<T, C> {
    abstract val config: Warehouse.Config
    abstract fun getTableReference(tableName: String): String
    abstract fun createConnection(): C
    abstract fun init()
    abstract fun resultSetFor(query: String): ResultSet
}
