package com.metriql.warehouse.postgresql

import com.metriql.tests.JdbcTestDataSource

class TestDataSourcePostgresql : JdbcTestDataSource() {
    override val testingServer = TestingEnvironmentPostgresql
}
