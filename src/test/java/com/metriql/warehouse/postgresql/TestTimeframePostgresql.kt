package com.metriql.warehouse.postgresql

import com.metriql.tests.TestTimeframe

class TestTimeframePostgresql : TestTimeframe() {
    override val testingServer = TestingEnvironmentPostgresql
}
