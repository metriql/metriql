package com.metriql.db.snowflake

import com.metriql.postoperation.TestPostOperation
import com.metriql.warehouse.snowflake.SnowflakeMetriqlBridge
import java.time.format.DateTimeFormatter

class TestPostOperationSnowflake : TestPostOperation() {
    override val testingServer = TestingEnvironmentSnowflake
    override val warehouseBridge = SnowflakeMetriqlBridge

    override val timestampColumn = "CAST('$timestamp' AS TIMESTAMP)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
