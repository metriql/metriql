package com.metriql.warehouse.mssqlserver

import com.metriql.tests.TestPostOperation

class TestPostOperationMSSQLServer : TestPostOperation() {
    override val testingServer = TestingEnvironmentMSSQLServer
}
