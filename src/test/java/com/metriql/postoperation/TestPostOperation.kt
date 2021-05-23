package com.metriql.postoperation

import com.metriql.db.TestingServer
import com.metriql.util.`try?`
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import org.testng.annotations.Test
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import kotlin.test.assertEquals

abstract class TestPostOperation {
    abstract val testingServer: TestingServer<*, *>
    abstract val warehouseBridge: WarehouseMetriqlBridge

    val timestamp = Instant.ofEpochSecond(1286705410)
    val date = LocalDate.of(2010, 10, 10)
    val time = LocalTime.of(11, 12, 13)

    abstract val timestampColumn: String
    abstract val dateColumn: String
    abstract val timeColumn: String

    @Test
    open fun `test timestamp post operation hour`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.HOUR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getTimestamp(1)
        assertEquals(result.toInstant().toString(), "2010-10-10T10:00:00Z")
    }

    @Test
    fun `test timestamp post operation day`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = `try?` { rs.getDate(1).toLocalDate().toString() } ?: LocalDate.parse(rs.getString(1)).toString()
        assertEquals("2010-10-10", result)
    }

    @Test
    fun `test timestamp post operation week`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = `try?` { rs.getDate(1).toLocalDate().toString() } ?: LocalDate.parse(rs.getString(1)).toString()
        assertEquals("2010-10-10", result)
    }

    @Test
    fun `test timestamp post operation month`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = `try?` { rs.getDate(1).toLocalDate().toString() } ?: LocalDate.parse(rs.getString(1)).toString()
        assertEquals("2010-10-01", result)
    }

    @Test
    fun `test timestamp post operation year`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = `try?` { rs.getDate(1).toLocalDate().toString() } ?: LocalDate.parse(rs.getString(1)).toString()
        assertEquals("2010-01-01", result)
    }

    @Test
    fun `test timestamp post operation hour of day`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.HOUR_OF_DAY]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(LocalTime.parse(result), LocalTime.of(10, 0, 0))
    }

    @Test
    fun `test timestamp post operation day of week`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY_OF_WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(result, "Sunday")
    }

    @Test
    fun `test timestamp post operation day of month`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY_OF_MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getInt(1)
        assertEquals(result, 10)
    }

    @Test
    fun `test timestamp post operation week of year`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.WEEK_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getInt(1)
        assertEquals(40, result)
    }

    @Test
    fun `test timestamp post operation month of year`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.MONTH_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(result, "October")
    }

    @Test
    fun `test timestamp post operation quarter of year`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.QUARTER_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(result, "Q4")
    }

    @Test
    fun `test date post operation week`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = `try?` { rs.getDate(1).toLocalDate().toString() } ?: LocalDate.parse(rs.getString(1)).toString()
        assertEquals("2010-10-04", result)
    }

    @Test
    fun `test date post operation month`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = `try?` { rs.getDate(1).toLocalDate().toString() } ?: LocalDate.parse(rs.getString(1)).toString()
        assertEquals("2010-10-01", result)
    }

    @Test
    fun `test date post operation year`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = `try?` { rs.getDate(1).toLocalDate().toString() } ?: LocalDate.parse(rs.getString(1)).toString()
        assertEquals("2010-01-01", result)
    }

    @Test
    fun `test date post operation day of week`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.DAY_OF_WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(result, "Sunday")
    }

    @Test
    fun `test date post operation day of month`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.DAY_OF_MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getInt(1)
        assertEquals(result, 10)
    }

    @Test
    fun `test date post operation week of year`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.WEEK_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getInt(1)
        assertEquals(40, result)
    }

    @Test
    fun `test date post operation month of year`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.MONTH_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(result, "October")
    }

    @Test
    fun `test date post operation quarter of year`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.QUARTER_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(result, "Q4")
    }

    @Test
    fun `test time post operation minute`() {
        val template = warehouseBridge.timeframes.timePostOperations[TimePostOperation.MINUTE]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timeColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(LocalTime.parse(result), LocalTime.of(11, 12, 0))
    }

    @Test
    fun `test time post operation hour`() {
        val template = warehouseBridge.timeframes.timePostOperations[TimePostOperation.HOUR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timeColumn)}"
        val rs = testingServer.resultSetFor(query)
        rs.next()
        val result = rs.getString(1)
        assertEquals(LocalTime.parse(result), LocalTime.of(11, 0, 0))
    }
}
