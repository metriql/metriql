package com.metriql.warehouse.duckdb

import com.metriql.tests.JdbcTestSimpleFilter
import java.sql.Statement

class TestSimpleFilterDuckdb : JdbcTestSimpleFilter() {
    override val testingServer = TestingEnvironmentEmbeddedDuckdb

    override fun setTimezone(stmt: Statement) {
//        super.setTimezone(stmt)
    }

    override fun populate() {
        // no-op
    }
}
