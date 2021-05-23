package com.metriql.db.presto

import com.metriql.postoperation.TestPostOperation
import com.metriql.warehouse.presto.PrestoMetriqlBridge
import java.time.format.DateTimeFormatter

class TestPostOperationPresto : TestPostOperation() {
    override val testingServer = TestingEnvironmentPresto
    override val warehouseBridge = PrestoMetriqlBridge

    override val timestampColumn = "CAST('$timestamp' AS TIMESTAMP)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"
}
