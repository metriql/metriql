package com.metriql.tests

import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Model
import com.metriql.service.model.Model.Dimension.DimensionValue.Column
import com.metriql.service.model.Model.Dimension.Type.COLUMN
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import org.testng.annotations.Test
import java.time.ZoneId
import kotlin.test.assertEquals
import kotlin.test.fail

abstract class TestSimpleFilter {

    val table = "filter_tests"
    abstract val dataSource: DataSource
    abstract val testingServer: TestingServer<*, *>
    abstract fun populate()

    open val doubleType: String = "double"

    open fun castToDouble(value: String): String {
        return "CAST($value as $doubleType)"
    }

    private fun context(): QueryGeneratorContext {
        return QueryGeneratorContext(
            ProjectAuth.singleProject(timezone = ZoneId.of("UTC")), dataSource,
            TestDatasetService(
                listOf(
                    Model(
                        "filter_tests", false,
                        Model.Target.initWithTable(null, "rakam_test", "filter_tests"),
                        null, null, null, Model.MappingDimensions.build(Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP to "_time"), listOf(),
                        listOf(
                            Model.Dimension("test_int", COLUMN, Column("test_int"), fieldType = FieldType.INTEGER),
                            Model.Dimension("test_string", COLUMN, Column("test_string"), fieldType = FieldType.STRING),
                            Model.Dimension("test_double", COLUMN, Column("test_double"), fieldType = FieldType.DOUBLE),
                            Model.Dimension("test_date", COLUMN, Column("test_date"), fieldType = FieldType.DATE),
                            Model.Dimension("test_bool", COLUMN, Column("test_bool"), fieldType = FieldType.BOOLEAN),
                            Model.Dimension("test_timestamp", COLUMN, Column("test_timestamp"), fieldType = FieldType.TIMESTAMP),
                        ),
                        listOf(),
                    )
                )
            ),
            JinjaRendererService(), null, null, null
        )
    }

    private val warehouseBridge: WarehouseMetriqlBridge
        get() = dataSource.warehouse.bridge

    private fun runQuery(query: String): List<Any?>? {
        val task = dataSource.createQueryTask(
            ProjectAuth.singleProject(ZoneId.of("UTC")).warehouseAuth(),
            QueryResult.QueryStats.QueryInfo.rawSql(query),
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
    fun testAnyFiltersIsSet() {
        val test = SimpleFilterTests.AnyOperatorTest.IS_SET
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testAnyFiltersIsNotSet() {
        val test = SimpleFilterTests.AnyOperatorTest.IS_NOT_SET
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testStringFiltersEquals() {
        val test = SimpleFilterTests.StringOperatorTest.EQUALS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testStringFiltersNotEquals() {
        val test = SimpleFilterTests.StringOperatorTest.NOT_EQUALS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testStringFiltersContains() {
        val test = SimpleFilterTests.StringOperatorTest.CONTAINS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testStringFiltersEndsWith() {
        val test = SimpleFilterTests.StringOperatorTest.ENDS_WITH
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testStringFiltersStartsWith() {
        val test = SimpleFilterTests.StringOperatorTest.STARTS_WITH
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testStringFiltersIn() {
        val test = SimpleFilterTests.StringOperatorTest.IN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testStringFiltersEqualsMulti() {
        val test = SimpleFilterTests.StringOperatorTest.EQUALS_MULTI
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testBooleanFiltersEquals() {
        val test = SimpleFilterTests.BooleanTest.EQUALS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("count(*)")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testBooleanFiltersNotEquals() {
        val test = SimpleFilterTests.BooleanTest.NOT_EQUALS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("count(*)")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testNumberFiltersEqualsInt() {
        val test = SimpleFilterTests.NumberTest.EQUALS_INT
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_int")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testNumberFiltersEqualsDouble() {
        val test = SimpleFilterTests.NumberTest.EQUALS_DOUBLE
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_double")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testNumberFiltersGreaterThanInt() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_INT
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_int")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_int"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testNumberFiltersGreaterThanDouble() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_DOUBLE
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT ${castToDouble("test_double")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_double"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testNumberFiltersLessThanInt() {
        val test = SimpleFilterTests.NumberTest.LESS_THAN_INT
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_int")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testNumberFiltersLessThanDouble() {
        val test = SimpleFilterTests.NumberTest.LESS_THAN_DOUBLE
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_double")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testNumberFiltersGreaterThanAndLessThanInt() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_AND_LESS_THAN_INT
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT ${castToDouble("test_double")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_double"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testNumberFiltersGreaterThanAndLessThanDouble() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_AND_LESS_THAN_DOUBLE
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT ${castToDouble("test_double")} FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_double"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testTimestampGreaterThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.GREATER_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_timestamp FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"

        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testTimestampLessThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.LESS_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_timestamp FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testTimestampGreaterThanAndLessThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.GREATER_THAN_AND_LESS_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_timestamp FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testDateGreaterThan() {
        val test = SimpleFilterTests.DateOperatorTests.GREATER_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_date FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testDateLessThan() {
        val test = SimpleFilterTests.DateOperatorTests.LESS_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_date FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testDateGreaterThanAndLessThan() {
        val test = SimpleFilterTests.DateOperatorTests.GREATER_THAN_AND_LESS_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_date FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testComplexFilters1() {
        val test = SimpleFilterTests.ComplexTest.COMPLEX_1
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }

    @Test
    fun testComplexFilters2() {
        val test = SimpleFilterTests.ComplexTest.COMPLEX_2
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context()) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        assertEquals(test.result, runQuery(query))
    }
}
