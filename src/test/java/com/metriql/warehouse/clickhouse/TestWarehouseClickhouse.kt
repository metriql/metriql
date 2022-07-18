package com.metriql.warehouse.clickhouse

import com.metriql.tests.JdbcTestWarehouse
import org.testng.annotations.Test

class TestWarehouseClickhouse : JdbcTestWarehouse(useIntsForBoolean = true) {
    override val testingServer = TestingEnvironmentClickhouse
    override val tableDefinition = "ENGINE = Memory"

    @Test
    fun testName() {
        testingServer.runQueryFirstRow("select 1")
    }
}
