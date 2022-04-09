package com.metriql.warehouse.presto

import com.metriql.tests.JdbcTestSimpleFilter

class TestSimpleFilterPresto : JdbcTestSimpleFilter() {
    override val testingServer = TestingEnvironmentPresto
}
