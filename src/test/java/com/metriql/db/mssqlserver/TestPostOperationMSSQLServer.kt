package com.metriql.db.mssqlserver

import com.metriql.tests.TestPostOperation
import com.metriql.warehouse.mssqlserver.MSSQLDataSource
import java.time.format.DateTimeFormatter

class TestPostOperationMSSQLServer : TestPostOperation() {
    override val testingServer = TestingEnvironmentMSSQLServer
    override val dataSource = MSSQLDataSource(testingServer.config)

    override val timestampColumn = "CAST('$timestamp' AS DATETIME)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
