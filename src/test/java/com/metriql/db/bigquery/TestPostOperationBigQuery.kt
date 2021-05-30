package com.metriql.db.bigquery

import com.metriql.postoperation.TestPostOperation
import com.metriql.warehouse.bigquery.BigQueryDataSource
import com.metriql.warehouse.spi.function.TimestampPostOperation
import org.testng.annotations.Test
import java.time.Instant
import java.time.format.DateTimeFormatter
import kotlin.test.assertEquals

class TestPostOperationBigQuery : TestPostOperation() {
    override val testingServer = TestingEnvironmentBigQuery
    override val dataSource = BigQueryDataSource(testingServer.config)

    override val timestampColumn = "CAST('$timestamp' AS TIMESTAMP)"
    override val dateColumn = "CAST('${date.format(DateTimeFormatter.ISO_DATE)}' AS DATE)"
    override val timeColumn = "CAST('$time' AS TIME)"

    @Test
    override fun `test timestamp post operation hour`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.HOUR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)

        assertEquals(Instant.parse(result).toString(), "2010-10-10T10:00:00Z")
    }
}
