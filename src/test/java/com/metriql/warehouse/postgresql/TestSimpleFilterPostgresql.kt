package com.metriql.warehouse.postgresql

import com.metriql.tests.JdbcTestSimpleFilter
import java.sql.Statement

class TestSimpleFilterPostgresql : JdbcTestSimpleFilter() {
    override val testingServer = TestingEnvironmentPostgresql

    override fun setTimezone(stmt: Statement) {
        stmt.execute("SET time zone 'UTC'")
    }
}
