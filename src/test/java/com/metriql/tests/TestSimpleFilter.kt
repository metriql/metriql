package com.metriql.tests

import com.metriql.any
import com.metriql.db.TestingServer
import com.metriql.service.model.DimensionName
import com.metriql.service.model.Model
import com.metriql.service.model.ModelDimension
import com.metriql.service.model.ModelName
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import org.mockito.Matchers
import org.mockito.Mockito.`when`
import org.mockito.Mockito.mock
import org.testng.annotations.Test
import java.sql.Date
import java.sql.Timestamp
import kotlin.test.assertEquals

abstract class TestSimpleFilter {

    val table = "filter_tests"

    abstract val warehouseBridge: WarehouseMetriqlBridge
    abstract val testingServer: TestingServer<*, *>
    abstract fun populate()

    private val context = mock(IQueryGeneratorContext::class.java).let {
        `when`(it.getModelDimension(Matchers.anyString(), Matchers.anyString())).thenAnswer { p0 ->
            val dimensionName = p0!!.arguments!!.first() as DimensionName
            val modelName = p0.arguments[1] as ModelName
            ModelDimension(
                modelName, Model.Target.initWithTable(null, null, modelName),
                Model.Dimension(
                    dimensionName,
                    Model.Dimension.Type.COLUMN,
                    Model.Dimension.DimensionValue.Column(dimensionName),
                    null,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
        }

        `when`(it.getSQLReference(any(), Matchers.anyString(), Matchers.anyString(), Matchers.anyListOf(String::class.java), Matchers.anyObject())).thenAnswer { p0 ->
            val modelTarget = p0!!.arguments!!.first() as Model.Target
            // val modelName = p0.arguments!![1]
            val columnName = p0.arguments!![2]
            val table = (modelTarget.value as Model.Target.TargetValue.Table).table
            val aq = warehouseBridge.aliasQuote
            "$aq$table$aq.$aq$columnName$aq"
        }
        it
    }

    @Test
    fun testAnyFiltersIsSet() {
        val test = SimpleFilterTests.AnyOperatorTest.IS_SET
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testAnyFiltersIsNotSet() {
        val test = SimpleFilterTests.AnyOperatorTest.IS_NOT_SET
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testStringFiltersEquals() {
        val test = SimpleFilterTests.StringOperatorTest.EQUALS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testStringFiltersNotEquals() {
        val test = SimpleFilterTests.StringOperatorTest.NOT_EQUALS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testStringFiltersContains() {
        val test = SimpleFilterTests.StringOperatorTest.CONTAINS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testStringFiltersEndsWith() {
        val test = SimpleFilterTests.StringOperatorTest.ENDS_WITH
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testStringFiltersStartsWith() {
        val test = SimpleFilterTests.StringOperatorTest.STARTS_WITH
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testStringFiltersIn() {
        val test = SimpleFilterTests.StringOperatorTest.IN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testStringFiltersEqualsMulti() {
        val test = SimpleFilterTests.StringOperatorTest.EQUALS_MULTI
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query =
            "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_string"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testBooleanFiltersIs() {
        val test = SimpleFilterTests.BooleanTest.IS
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Int>()
        val query = "SELECT count(*) FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getInt(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testNumberFiltersEqualsInt() {
        val test = SimpleFilterTests.NumberTest.EQUALS_INT
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Int>()
        val query = "SELECT test_int FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getInt(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testNumberFiltersEqualsDouble() {
        val test = SimpleFilterTests.NumberTest.EQUALS_DOUBLE
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Double>()
        val query = "SELECT test_double FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getDouble(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testNumberFiltersGreaterThanInt() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_INT
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Int>()
        val query = "SELECT test_int FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_int"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getInt(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testNumberFiltersGreaterThanDouble() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_DOUBLE
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Double>()
        val query =
            "SELECT test_double FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_double"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getDouble(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testNumberFiltersLessThanInt() {
        val test = SimpleFilterTests.NumberTest.LESS_THAN_INT
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Int>()
        val query = "SELECT test_int FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getInt(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testNumberFiltersLessThanDouble() {
        val test = SimpleFilterTests.NumberTest.LESS_THAN_DOUBLE
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Double>()
        val query = "SELECT test_double FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getDouble(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testNumberFiltersGreaterThanAndLessThanInt() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_AND_LESS_THAN_INT
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Int>()
        val query =
            "SELECT test_double FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_double"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getInt(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testNumberFiltersGreaterThanAndLessThanDouble() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_AND_LESS_THAN_DOUBLE
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Double>()
        val query =
            "SELECT test_double FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter ORDER BY test_double"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getDouble(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testTimestampGreaterThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.GREATER_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Timestamp>()
        val query =
            "SELECT test_timestamp FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getTimestamp(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testTimestampLessThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.LESS_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Timestamp>()
        val query =
            "SELECT test_timestamp FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getTimestamp(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testTimestampGreaterThanAndLessThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.GREATER_THAN_AND_LESS_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Timestamp>()
        val query =
            "SELECT test_timestamp FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getTimestamp(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testDateGreaterThan() {
        val test = SimpleFilterTests.DateOperatorTests.GREATER_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Date>()
        val query = "SELECT test_date FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getDate(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testDateLessThan() {
        val test = SimpleFilterTests.DateOperatorTests.LESS_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Date>()
        val query = "SELECT test_date FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getDate(1))
        }
        assert((test.result as List<*>).size == results.size)
        ((test.result as List<*>) zip results).forEach { (testResult, result) ->
            assertEquals(testResult.toString(), result.toString())
        }
    }

    @Test
    fun testDateGreaterThanAndLessThan() {
        val test = SimpleFilterTests.DateOperatorTests.GREATER_THAN_AND_LESS_THAN
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<Date>()
        val query = "SELECT test_date FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getDate(1))
        }
        assert((test.result as List<*>).size == results.size)
        ((test.result as List<*>) zip results).forEach { (testResult, result) ->
            assertEquals(testResult.toString(), result.toString())
        }
    }

    @Test
    fun testComplexFilters1() {
        val test = SimpleFilterTests.ComplexTest.COMPLEX_1
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }

    @Test
    fun testComplexFilters2() {
        val test = SimpleFilterTests.ComplexTest.COMPLEX_2
        val generatedFilter = test.filter(table)
            .map { warehouseBridge.renderFilter(it, table, context, null) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val results = mutableListOf<String>()
        val query = "SELECT test_string FROM ${testingServer.getTableReference(table)} WHERE $generatedFilter"
        val rs = testingServer.resultSetFor(query)
        while (rs.next()) {
            results.add(rs.getString(1))
        }
        assertEquals(test.result, results)
    }
}
