package com.metriql.warehouse.presto

import com.metriql.tests.TestTimeframe

class TestTimeframePresto : TestTimeframe() {
    override val testingServer = TestingEnvironmentPresto
}
