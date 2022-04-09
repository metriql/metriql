package com.metriql.warehouse.postgresql

import com.metriql.tests.TestPostOperation

class TestPostOperationPostgresql : TestPostOperation() {
    override val testingServer = TestingEnvironmentPostgresql
}
