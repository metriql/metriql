package com.metriql.warehouse.mysql

import com.metriql.tests.JdbcTestWarehouse

class TestWarehouseMysql : JdbcTestWarehouse() {
    override val testingServer = TestingEnvironmentMySQL
}
