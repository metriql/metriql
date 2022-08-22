package com.metriql.warehouse.snowflake

import com.metriql.tests.TestTimeframe

class TestTimeframeSnowflake : TestTimeframe() {
    override val testingServer = TestingEnvironmentSnowflake
}
