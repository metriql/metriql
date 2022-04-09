package com.metriql.warehouse.mssqlserver

import com.metriql.tests.JdbcTestWarehouse

class TestWarehouseMSSQLServer : JdbcTestWarehouse() {
    override val testingServer = TestingEnvironmentMSSQLServer
}
