package com.metriql.db.mysql

import com.metriql.tests.TestPostOperation
import com.metriql.warehouse.mysql.MySQLDataSource
import java.time.format.DateTimeFormatter

class TestPostOperationMySQL : TestPostOperation() {
    override val testingServer = TestingEnvironmentMySQL
    override val dataSource = MySQLDataSource(testingServer.config)
    override val timestampColumn = "CAST('$timestamp' AS DATETIME)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
