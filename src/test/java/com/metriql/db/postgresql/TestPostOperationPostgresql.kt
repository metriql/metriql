package com.metriql.db.postgresql

import com.metriql.postoperation.TestPostOperation
import com.metriql.warehouse.postgresql.PostgresqlMetriqlBridge
import java.time.format.DateTimeFormatter

class TestPostOperationPostgresql : TestPostOperation() {
    override val testingServer = TestingEnvironmentPostgresql
    override val warehouseBridge = PostgresqlMetriqlBridge

    override val timestampColumn = "CAST('$timestamp' AS TIMESTAMP)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
