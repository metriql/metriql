package com.metriql.db.mysql

import com.metriql.postoperation.TestPostOperation
import com.metriql.warehouse.mysql.MySQLMetriqlBridge
import java.time.format.DateTimeFormatter

class TestPostOperationMySQL : TestPostOperation() {
    override val testingServer = TestingEnvironmentMySQL
    override val warehouseBridge = MySQLMetriqlBridge
    override val timestampColumn = "CAST('$timestamp' AS DATETIME)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
