package com.metriql.warehouse.mysql

import com.metriql.tests.JdbcTestDataSource

class TestDataSourceMysql : JdbcTestDataSource() {
    override val testingServer = TestingEnvironmentMySQL
}
