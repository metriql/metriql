package com.metriql.warehouse.mssqlserver

import com.metriql.tests.JdbcTestDataSource

class TestDataSourceMSSQLServer : JdbcTestDataSource() {
    override val testingServer = TestingEnvironmentMSSQLServer
}
