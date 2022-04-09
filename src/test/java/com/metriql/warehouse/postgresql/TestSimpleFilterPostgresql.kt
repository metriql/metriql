package com.metriql.warehouse.postgresql

import com.metriql.tests.JdbcTestSimpleFilter

class TestSimpleFilterPostgresql : JdbcTestSimpleFilter() {
    override val testingServer = TestingEnvironmentPostgresql
}
