package com.metriql.warehouse.snowflake

import com.metriql.tests.JdbcTestSimpleFilter

class TestSimpleFilterSnowflake : JdbcTestSimpleFilter() {
    override val testingServer = TestingEnvironmentSnowflake
}
