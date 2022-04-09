package com.metriql.warehouse.mssqlserver

import com.metriql.tests.JdbcTestSimpleFilter

class TestSimpleFilterMSSQLServer : JdbcTestSimpleFilter() {
    override val testingServer = TestingEnvironmentMSSQLServer
}
