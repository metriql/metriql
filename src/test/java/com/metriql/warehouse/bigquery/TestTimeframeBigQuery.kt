package com.metriql.warehouse.bigquery

import com.metriql.tests.TestTimeframe
import com.metriql.warehouse.spi.function.TimestampPostOperation
import org.testng.annotations.Test
import java.time.Instant
import kotlin.test.assertEquals

class TestTimeframeBigQuery : TestTimeframe() {
    override val testingServer = TestingEnvironmentBigQuery

    @Test
    override fun `test timestamp timeframe hour`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.HOUR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.runQueryFirstRow(query)?.get(0)
        assertEquals(Instant.parse(rs.toString()).toString(), "2010-10-10T10:00:00Z")
    }
}
