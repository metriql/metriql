package com.metriql.warehouse.presto

import com.metriql.tests.TestPostOperation

class TestPostOperationPresto : TestPostOperation() {
    override val testingServer = TestingEnvironmentPresto
}
