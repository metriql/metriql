package com.metriql.warehouse.mysql

import com.metriql.tests.TestTimeframe

class TestTimeframeMySQL : TestTimeframe() {
    override val testingServer = TestingEnvironmentMySQL
}
