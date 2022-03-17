package com.metriql.db.snowflake

import com.metriql.tests.TestPostOperation
import com.metriql.warehouse.snowflake.SnowflakeDataSource
import java.time.format.DateTimeFormatter

class TestPostOperationSnowflake : TestPostOperation() {
    override val testingServer = TestingEnvironmentSnowflake
    override val dataSource = SnowflakeDataSource(testingServer.config)

    override val timestampColumn = "CAST('$timestamp' AS TIMESTAMP)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
