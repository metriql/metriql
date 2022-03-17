package com.metriql.db.postgresql

import com.metriql.tests.TestPostOperation
import com.metriql.warehouse.postgresql.PostgresqlDataSource
import java.time.format.DateTimeFormatter

class TestPostOperationPostgresql : TestPostOperation() {
    override val testingServer = TestingEnvironmentPostgresql
    override val dataSource = PostgresqlDataSource(testingServer.config)

    override val timestampColumn = "CAST('$timestamp' AS TIMESTAMP)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
