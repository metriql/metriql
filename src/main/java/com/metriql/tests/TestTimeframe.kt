package com.metriql.tests

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import io.trino.spi.type.StandardTypes
import org.testng.annotations.Test
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.test.assertEquals

abstract class TestTimeframe {
    abstract val testingServer: TestingServer<*>

    val timestamp: Instant = Instant.ofEpochSecond(1286705410)
    val date: LocalDate = LocalDate.of(2010, 10, 10)
    val time: LocalTime = LocalTime.of(11, 12, 13)

    val timestampColumn: String get() = "CAST('$timestamp' AS ${testingServer.bridge.mqlTypeMap[StandardTypes.TIMESTAMP]})"
    val dateColumn: String get() = "CAST('$date' AS ${testingServer.bridge.mqlTypeMap[StandardTypes.DATE]})"
    val timeColumn: String get() = "CAST('$time' AS ${testingServer.bridge.mqlTypeMap[StandardTypes.TIME]})"

    @Test
    open fun `test timestamp timeframe hour`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.HOUR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), LocalDateTime.parse("2010-10-10T10:00"))
    }

    @Test
    fun `test timestamp timeframe day`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(LocalDate.parse("2010-10-10"), testingServer.runQueryFirstRow(query)?.get(0))
    }

    @Test
    fun `test timestamp timeframe week`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        // TODO: verify
        assertEquals(LocalDate.parse("2010-10-04"), testingServer.runQueryFirstRow(query)?.get(0))
    }

    @Test
    fun `test timestamp timeframe month`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(LocalDate.parse("2010-10-01"), testingServer.runQueryFirstRow(query)?.get(0))
    }

    @Test
    fun `test timestamp timeframe year`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(LocalDate.parse("2010-01-01"), testingServer.runQueryFirstRow(query)?.get(0))
    }

    @Test
    fun `test timestamp timeframe hour of day`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.HOUR_OF_DAY]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), LocalTime.of(10, 0, 0))
    }

    @Test
    fun `test timestamp timeframe day of week`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY_OF_WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), "Sunday")
    }

    @Test
    fun `test timestamp timeframe day of month`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY_OF_MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals((testingServer.runQueryFirstRow(query)?.get(0) as? Number)?.toInt(), 10)
    }

    @Test
    fun `test timestamp timeframe week of year`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.WEEK_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(40, (testingServer.runQueryFirstRow(query)?.get(0) as? Number)?.toInt())
    }

    @Test
    fun `test timestamp timeframe month of year`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.MONTH_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), "October")
    }

    @Test
    fun `test timestamp timeframe quarter of year`() {
        val template = testingServer.bridge.timeframes.timestampPostOperations[TimestampPostOperation.QUARTER_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), "Q4")
    }

    @Test
    fun `test date timeframe week`() {
        val template = testingServer.bridge.timeframes.datePostOperations[DatePostOperation.WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(LocalDate.parse("2010-10-04"), testingServer.runQueryFirstRow(query)?.get(0))
    }

    @Test
    fun `test date timeframe month`() {
        val template = testingServer.bridge.timeframes.datePostOperations[DatePostOperation.MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(LocalDate.parse("2010-10-01"), testingServer.runQueryFirstRow(query)?.get(0))
    }

    @Test
    fun `test date timeframe year`() {
        val template = testingServer.bridge.timeframes.datePostOperations[DatePostOperation.YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(LocalDate.parse("2010-01-01"), testingServer.runQueryFirstRow(query)?.get(0))
    }

    @Test
    fun `test date timeframe day of week`() {
        val template = testingServer.bridge.timeframes.datePostOperations[DatePostOperation.DAY_OF_WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), "Sunday")
    }

    @Test
    fun `test date timeframe day of month`() {
        val template = testingServer.bridge.timeframes.datePostOperations[DatePostOperation.DAY_OF_MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals((testingServer.runQueryFirstRow(query)?.get(0) as? Number)?.toInt(), 10)
    }

    @Test
    fun `test date timeframe week of year`() {
        val template = testingServer.bridge.timeframes.datePostOperations[DatePostOperation.WEEK_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(40, (testingServer.runQueryFirstRow(query)?.get(0) as? Number)?.toInt())
    }

    @Test
    fun `test date timeframe month of year`() {
        val template = testingServer.bridge.timeframes.datePostOperations[DatePostOperation.MONTH_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), "October")
    }

    @Test
    fun `test date timeframe quarter of year`() {
        val template = testingServer.bridge.timeframes.datePostOperations[DatePostOperation.QUARTER_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), "Q4")
    }

    @Test
    fun `test time timeframe minute`() {
        val template = testingServer.bridge.timeframes.timePostOperations[TimePostOperation.MINUTE]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timeColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), LocalTime.of(11, 12, 0))
    }

    @Test
    fun `test time timeframe hour`() {
        val template = testingServer.bridge.timeframes.timePostOperations[TimePostOperation.HOUR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timeColumn)}"
        assertEquals(testingServer.runQueryFirstRow(query)?.get(0), LocalTime.of(11, 0, 0))
    }
}
