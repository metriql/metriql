package com.metriql.db.mssqlserver

import com.metriql.postoperation.TestPostOperation
import com.metriql.warehouse.mssqlserver.MSSQLMetriqlBridge
import java.time.format.DateTimeFormatter

class TestPostOperationMSSQLServer : TestPostOperation() {
    override val testingServer = TestingEnvironmentMSSQLServer
    override val warehouseBridge = MSSQLMetriqlBridge

    override val timestampColumn = "CAST('$timestamp' AS DATETIME)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
