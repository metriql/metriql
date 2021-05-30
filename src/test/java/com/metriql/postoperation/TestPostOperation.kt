package com.metriql.postoperation

import com.metriql.db.TestingServer
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import org.testng.annotations.Test
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import kotlin.test.assertEquals
import kotlin.test.fail

abstract class TestPostOperation {
    abstract val testingServer: TestingServer<*, *>
    abstract val dataSource: DataSource

    val timestamp = Instant.ofEpochSecond(1286705410)
    val date = LocalDate.of(2010, 10, 10)
    val time = LocalTime.of(11, 12, 13)

    abstract val timestampColumn: String
    abstract val dateColumn: String
    abstract val timeColumn: String

    protected val warehouseBridge: WarehouseMetriqlBridge
        get() = dataSource.warehouse.bridge

    private fun runQuery(query: String): List<Any?>? {
        val task = dataSource.createQueryTask(
            ProjectAuth.singleProject(ZoneId.of("UTC")).warehouseAuth(),
            query,
            null,
            null,
            null,
            false
        ).runAndWaitForResult()
        if (task.error != null) {
            fail("Error running query: $query \n ${task.error}")
        }

        return if (task.result?.isEmpty() == true) {
            null
        } else {
            task.result?.get(0)
        }
    }

    @Test
    open fun `test timestamp post operation hour`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.HOUR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(runQuery(query)?.get(0), LocalDateTime.parse("2010-10-10T10:00"))
    }

    @Test
    fun `test timestamp post operation day`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(LocalDate.parse("2010-10-10"), runQuery(query)?.get(0))
    }

    @Test
    fun `test timestamp post operation week`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(LocalDate.parse("2010-10-10"), runQuery(query)?.get(0))
    }

    @Test
    fun `test timestamp post operation month`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(LocalDate.parse("2010-10-01"), runQuery(query)?.get(0))
    }

    @Test
    fun `test timestamp post operation year`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(LocalDate.parse("2010-01-01"), runQuery(query)?.get(0))
    }

    @Test
    fun `test timestamp post operation hour of day`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.HOUR_OF_DAY]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(runQuery(query)?.get(0), LocalTime.of(10, 0, 0))
    }

    @Test
    fun `test timestamp post operation day of week`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY_OF_WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(runQuery(query)?.get(0), "Sunday")
    }

    @Test
    fun `test timestamp post operation day of month`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.DAY_OF_MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals((runQuery(query)?.get(0) as? Number)?.toInt(), 10)
    }

    @Test
    fun `test timestamp post operation week of year`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.WEEK_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(40, (runQuery(query)?.get(0) as? Number)?.toInt())
    }

    @Test
    fun `test timestamp post operation month of year`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.MONTH_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(runQuery(query)?.get(0), "October")
    }

    @Test
    fun `test timestamp post operation quarter of year`() {
        val template = warehouseBridge.timeframes.timestampPostOperations[TimestampPostOperation.QUARTER_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timestampColumn)}"
        assertEquals(runQuery(query)?.get(0), "Q4")
    }

    @Test
    fun `test date post operation week`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(LocalDate.parse("2010-10-04"), runQuery(query)?.get(0))
    }

    @Test
    fun `test date post operation month`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(LocalDate.parse("2010-10-01"), runQuery(query)?.get(0))
    }

    @Test
    fun `test date post operation year`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(LocalDate.parse("2010-01-01"), runQuery(query)?.get(0))
    }

    @Test
    fun `test date post operation day of week`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.DAY_OF_WEEK]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(runQuery(query)?.get(0), "Sunday")
    }

    @Test
    fun `test date post operation day of month`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.DAY_OF_MONTH]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals((runQuery(query)?.get(0) as? Number)?.toInt(), 10)
    }

    @Test
    fun `test date post operation week of year`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.WEEK_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(40, (runQuery(query)?.get(0) as? Number)?.toInt())
    }

    @Test
    fun `test date post operation month of year`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.MONTH_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(runQuery(query)?.get(0), "October")
    }

    @Test
    fun `test date post operation quarter of year`() {
        val template = warehouseBridge.timeframes.datePostOperations[DatePostOperation.QUARTER_OF_YEAR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, dateColumn)}"
        assertEquals(runQuery(query)?.get(0), "Q4")
    }

    @Test
    fun `test time post operation minute`() {
        val template = warehouseBridge.timeframes.timePostOperations[TimePostOperation.MINUTE]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timeColumn)}"
        assertEquals(runQuery(query)?.get(0), LocalTime.of(11, 12, 0))
    }

    @Test
    fun `test time post operation hour`() {
        val template = warehouseBridge.timeframes.timePostOperations[TimePostOperation.HOUR]
        if (template == null) {
            assert(true)
            return
        }
        val query = "SELECT ${String.format(template, timeColumn)}"
        assertEquals(runQuery(query)?.get(0), LocalTime.of(11, 0, 0))
    }
}
