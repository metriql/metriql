package com.metriql.warehouse.mysql

import com.metriql.tests.JdbcTestSimpleFilter

class TestSimpleFilterMySQL : JdbcTestSimpleFilter() {
    override val testingServer = TestingEnvironmentMySQL
}
