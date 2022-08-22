package com.metriql.warehouse.clickhouse

import com.metriql.tests.JdbcTestDataSource
import org.testng.annotations.Test

class TestDataSourceClickhouse : JdbcTestDataSource(useIntsForBoolean = true) {
    override val testingServer = TestingEnvironmentClickhouse
    override val tableDefinition = "ENGINE = Memory"

    @Test
    fun testName() {
        testingServer.runQueryFirstRow("select 1")
    }
}
