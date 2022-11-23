package com.metriql.tests

import com.metriql.db.FieldType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.Dataset
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.tests.Helper.assetEqualsCaseInsensitive
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import org.testng.Assert.assertEquals
import org.testng.annotations.Test
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId

abstract class TestDataSource<T> {
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
    val testDouble = SampleDataset.testInt.map { it * 1.0 }
    val testDate = SampleDataset.testInt.map { LocalDate.of(2000, 1, it + 1) }
    val testTimestamp = SampleDataset.testInt.map { Instant.ofEpochMilli((it * 1000 * 60 * 60).toLong()) }
    val testTime = SampleDataset.testInt.map { LocalTime.of(it, it) }

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
        val datasetTarget = Dataset.Target(Dataset.Target.Type.TABLE, Dataset.Target.TargetValue.Table("a", "b", "c"))
        val sqlTarget = testingServer.dataSource.sqlReferenceForTarget(datasetTarget, "model") { "" }
        assertEquals("\"a\".\"b\".\"c\" AS \"model\"", sqlTarget)
    }

    @Test
    open fun `test fill defaults`() {
        val datasetTarget = Dataset.Target(Dataset.Target.Type.TABLE, Dataset.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledDatasetTarget = testingServer.dataSource.fillDefaultsToTarget(datasetTarget).value as Dataset.Target.TargetValue.Table
        val config = testingServer.dataSource.config
        assertEquals(filledDatasetTarget.database, config.warehouseDatabase())
        assertEquals(filledDatasetTarget.schema, config.warehouseSchema())
    }

    @Test
    open fun `test get tables of a schema`() {
        val expected = testingServer.dataSource.listTableNames(null, null).first()
        assetEqualsCaseInsensitive(expected, tableName)
    }

    @Test
    open fun `test table schema by sql`() {
        val query = """
        SELECT 1 as test_int,
               'test' as test_string,
               1.1 as test_double,
               ${testingServer.bridge.filters.parseAnyValue(LocalDate.parse("1970-01-01"), context, FieldType.DATE)} as test_date,
               TRUE as test_bool,
               ${testingServer.bridge.filters.parseAnyValue(LocalDate.parse("1970-01-01"), context, FieldType.TIMESTAMP)} as test_timestamp,
               ${testingServer.bridge.filters.parseAnyValue(LocalTime.parse("15:30:00"), context, FieldType.TIME)} as test_time
        """.trimIndent()
        testingServer.dataSource.getTableSchema(query).columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
        assert(true)
    }

    @Test
    fun `test table schema by table reference`() {
        val tableSchema = testingServer.dataSource
            .getTableSchema(null, null, tableName)
        assetEqualsCaseInsensitive(tableSchema.name, tableName)
        tableSchema.columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
        assetEqualsCaseInsensitive(tableSchema.name, tableName)
    }

    @Test
    fun `test listing schema and field types of columns`() {
        val tableSchema = testingServer.dataSource.listSchema(null, null, null)
            .first()
        assetEqualsCaseInsensitive(tableSchema.name, tableName)
        tableSchema.columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
        assetEqualsCaseInsensitive(tableSchema.name, tableName)
    }
}
