package com.metriql.tests

import com.metriql.db.FieldType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Model
import com.metriql.service.model.Model.Dimension.DimensionValue.Column
import com.metriql.service.model.Model.Dimension.Type.COLUMN
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import io.trino.spi.type.StandardTypes
import org.testng.Assert.assertEquals
import org.testng.annotations.Test
import java.time.ZoneId

abstract class TestSimpleFilter<C> {

    val table = "filter_tests"
    abstract val testingServer: TestingServer<C>

    open fun castToDouble(value: String): String {
        return "CAST($value as ${testingServer.bridge.mqlTypeMap[StandardTypes.DOUBLE]})"
    }

    private fun context(): QueryGeneratorContext {
        return QueryGeneratorContext(
            ProjectAuth.singleProject(timezone = ZoneId.of("UTC")), testingServer.dataSource,
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

    @Test
    fun testAnyFiltersIsSet() {
        val test = SimpleFilterTests.AnyOperatorTest.IS_SET
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testAnyFiltersIsNotSet() {
        val test = SimpleFilterTests.AnyOperatorTest.IS_NOT_SET
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testStringFiltersEquals() {
        val test = SimpleFilterTests.StringOperatorTest.EQUALS
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testStringFiltersNotEquals() {
        val test = SimpleFilterTests.StringOperatorTest.NOT_EQUALS
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testStringFiltersContains() {
        val test = SimpleFilterTests.StringOperatorTest.CONTAINS
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testStringFiltersEndsWith() {
        val test = SimpleFilterTests.StringOperatorTest.ENDS_WITH
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testStringFiltersStartsWith() {
        val test = SimpleFilterTests.StringOperatorTest.STARTS_WITH
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testStringFiltersIn() {
        val test = SimpleFilterTests.StringOperatorTest.IN
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testStringFiltersEqualsMulti() {
        val test = SimpleFilterTests.StringOperatorTest.EQUALS_MULTI
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_string"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testBooleanFiltersEquals() {
        val test = SimpleFilterTests.BooleanTest.EQUALS
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("count(*)")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testBooleanFiltersNotEquals() {
        val test = SimpleFilterTests.BooleanTest.NOT_EQUALS
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("count(*)")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testNumberFiltersEqualsInt() {
        val test = SimpleFilterTests.NumberTest.EQUALS_INT
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_int")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testNumberFiltersEqualsDouble() {
        val test = SimpleFilterTests.NumberTest.EQUALS_DOUBLE
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_double")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testNumberFiltersGreaterThanInt() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_INT
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_int")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_int"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testNumberFiltersGreaterThanDouble() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_DOUBLE
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT ${castToDouble("test_double")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_double"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testNumberFiltersLessThanInt() {
        val test = SimpleFilterTests.NumberTest.LESS_THAN_INT
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_int")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testNumberFiltersLessThanDouble() {
        val test = SimpleFilterTests.NumberTest.LESS_THAN_DOUBLE
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT ${castToDouble("test_double")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testNumberFiltersGreaterThanAndLessThanInt() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_AND_LESS_THAN_INT
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT ${castToDouble("test_double")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_double"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testNumberFiltersGreaterThanAndLessThanDouble() {
        val test = SimpleFilterTests.NumberTest.GREATER_THAN_AND_LESS_THAN_DOUBLE
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT ${castToDouble("test_double")} FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter ORDER BY test_double"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testTimestampGreaterThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.GREATER_THAN
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_timestamp FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"

        val actual = testingServer.runQueryFirstRow(query)
        assertEquals(test.result, actual)
    }

    @Test
    fun testTimestampLessThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.LESS_THAN
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_timestamp FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testTimestampGreaterThanAndLessThan() {
        val test = SimpleFilterTests.TimestampOperatorTest.GREATER_THAN_AND_LESS_THAN
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query =
            "SELECT test_timestamp FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
                "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testDateGreaterThan() {
        val test = SimpleFilterTests.DateOperatorTests.GREATER_THAN
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_date FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testDateLessThan() {
        val test = SimpleFilterTests.DateOperatorTests.LESS_THAN
        val context = context()
        val generatedFilter = test.filter(table)
            .map {
                testingServer.bridge.renderFilter(it, table, context)
            }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_date FROM " +
            "${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testDateGreaterThanAndLessThan() {
        val test = SimpleFilterTests.DateOperatorTests.GREATER_THAN_AND_LESS_THAN
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_date FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testComplexFilters1() {
        val test = SimpleFilterTests.ComplexTest.COMPLEX_1
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }

    @Test
    fun testComplexFilters2() {
        val test = SimpleFilterTests.ComplexTest.COMPLEX_2
        val context = context()
        val generatedFilter = test.filter(table)
            .map { testingServer.bridge.renderFilter(it, table, context) }
            .joinToString(" AND ") { "(${it.whereFilter})" }
        val query = "SELECT test_string FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(test.result, testingServer.runQueryFirstRow(query))
    }
}
