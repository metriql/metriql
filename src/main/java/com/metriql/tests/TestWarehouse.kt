package com.metriql.tests

import com.metriql.db.FieldType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Model
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import org.testng.annotations.Test
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import kotlin.test.assertEquals

abstract class TestWarehouse<T> {
    abstract val testingServer: TestingServer<T>
    abstract val useIntsForBoolean: Boolean

    val tableName = "warehouse_test"
    val schemaName = "rakam_test"

    val testInt = listOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val testString = listOf("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliett")
    val testBool: List<Any> by lazy {
        val items = listOf(false, true, true, true, true, true, true, true, true, true)
        if (useIntsForBoolean) {
            items.map { if (it) 1 else 0 }
        } else items
    }
    val testDouble = SimpleFilterTests.testInt.map { it * 1.0 }
    val testDate = SimpleFilterTests.testInt.map { LocalDate.of(2000, 1, it + 1) }
    val testTimestamp = SimpleFilterTests.testInt.map { Instant.ofEpochMilli((it * 1000 * 60 * 60).toLong()) }
    val testTime = SimpleFilterTests.testInt.map { LocalTime.of(it, it) }

    val columnTypes = mapOf(
        "test_int" to listOf(FieldType.INTEGER, FieldType.LONG),
        "test_string" to listOf(FieldType.STRING),
        "test_double" to listOf(FieldType.DOUBLE, FieldType.DECIMAL),
        "test_date" to listOf(FieldType.DATE),
        "test_bool" to listOf(FieldType.BOOLEAN, FieldType.INTEGER, FieldType.LONG), // Mysql type boolean is long
        "test_timestamp" to listOf(FieldType.TIMESTAMP),
        "test_time" to listOf(FieldType.TIME)
    )

    val context by lazy {
        QueryGeneratorContext(
            ProjectAuth.singleProject(timezone = ZoneId.of("UTC")), testingServer.dataSource, TestDatasetService(),
            JinjaRendererService(), null, null, null
        )
    }

    abstract fun populate()

    @Test
    abstract fun `test list database names`()

    @Test
    abstract fun `test listing schema names`()

    @Test
    open fun `test connection test`() {
        assert(testingServer.dataSource.connectionTest(-1))
    }

    @Test
    open fun `test generate sql reference`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table("a", "b", "c"))
        val sqlTarget = testingServer.dataSource.sqlReferenceForTarget(modelTarget, "model") { "" }
        assertEquals("\"a\".\"b\".\"c\" AS \"model\"", sqlTarget)
    }

    @Test
    open fun `test fill defaults`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledModelTarget = testingServer.dataSource.fillDefaultsToTarget(modelTarget).value as Model.Target.TargetValue.Table
        assertEquals(filledModelTarget.database, "test_db")
        assertEquals("rakam_test", filledModelTarget.schema)
    }

    @Test
    open fun `test get tables of a schema`() {
        assertEquals(testingServer.dataSource.listTableNames(null, null).first(), tableName)
    }

    @Test
    open fun `test table schema by sql`() {
        val query = """
        SELECT 1 as test_int,
               'test' as test_string,
               1.1 as test_double,
               ${testingServer.bridge.filters.parseAnyValue(LocalDate.parse("1970-01-01"), context, FieldType.DATE)} as test_date,
               TRUE as test_bool,
               ${testingServer.bridge.filters.parseAnyValue(Instant.parse("1970-01-01"), context, FieldType.TIMESTAMP)} as test_timestamp,
               ${testingServer.bridge.filters.parseAnyValue(Instant.parse("15:30:00"), context, FieldType.TIME)} as test_time
        """.trimIndent()
        testingServer.dataSource.getTable(query).columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
        assert(true)
    }

    @Test
    fun `test table schema by table reference`() {
        val tableSchema = testingServer.dataSource
            .getTable(null, null, tableName)
        assertEquals(tableSchema.name, tableName)
        tableSchema.columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
        assertEquals(tableSchema.name, tableName)
    }

    @Test
    fun `test listing schema and field types of columns`() {
        val tableSchema = testingServer.dataSource.listSchema(null, null, null)
            .first()
        assertEquals(tableSchema.name, tableName)
        tableSchema.columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
        assertEquals(tableSchema.name, tableName)
    }
}
