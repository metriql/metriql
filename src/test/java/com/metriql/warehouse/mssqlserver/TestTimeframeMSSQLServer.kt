package com.metriql.warehouse.mssqlserver

import com.metriql.tests.TestTimeframe

class TestTimeframeMSSQLServer : TestTimeframe() {
    override val testingServer = TestingEnvironmentMSSQLServer
}
