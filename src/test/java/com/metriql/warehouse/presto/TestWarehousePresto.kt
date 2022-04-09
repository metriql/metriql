package com.metriql.warehouse.presto

import com.metriql.tests.JdbcTestWarehouse

class TestWarehousePresto : JdbcTestWarehouse() {
    override val testingServer = TestingEnvironmentPresto
}
