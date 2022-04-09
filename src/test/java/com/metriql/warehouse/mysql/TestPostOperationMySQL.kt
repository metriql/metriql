package com.metriql.warehouse.mysql

import com.metriql.tests.TestPostOperation

class TestPostOperationMySQL : TestPostOperation() {
    override val testingServer = TestingEnvironmentMySQL
}
