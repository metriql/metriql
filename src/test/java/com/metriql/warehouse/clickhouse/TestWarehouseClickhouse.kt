package com.metriql.warehouse.clickhouse

import com.metriql.tests.JdbcTestWarehouse

class TestWarehouseClickhouse : JdbcTestWarehouse(useIntsForBoolean = true) {
    override val testingServer = TestingEnvironmentClickhouse
    override val tableDefinition = "ENGINE = Memory"
}
