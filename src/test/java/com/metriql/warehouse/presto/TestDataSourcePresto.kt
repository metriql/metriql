package com.metriql.warehouse.presto

import com.metriql.tests.JdbcTestDataSource

class TestDataSourcePresto : JdbcTestDataSource() {
    override val testingServer = TestingEnvironmentPresto
}
