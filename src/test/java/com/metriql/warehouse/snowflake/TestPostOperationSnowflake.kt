package com.metriql.warehouse.snowflake

import com.metriql.tests.TestPostOperation

class TestPostOperationSnowflake : TestPostOperation() {
    override val testingServer = TestingEnvironmentSnowflake
}
