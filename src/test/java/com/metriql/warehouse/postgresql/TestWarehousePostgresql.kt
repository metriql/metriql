package com.metriql.warehouse.postgresql

import com.metriql.tests.JdbcTestWarehouse

class TestWarehousePostgresql : JdbcTestWarehouse() {
    override val testingServer = TestingEnvironmentPostgresql
}
