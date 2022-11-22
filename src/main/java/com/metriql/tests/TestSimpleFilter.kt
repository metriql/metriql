package com.metriql.tests

import com.metriql.report.data.ReportFilter
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.util.JsonHelper
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import io.trino.spi.type.StandardTypes
import org.intellij.lang.annotations.Language
import org.testng.Assert.assertEquals
import org.testng.annotations.BeforeTest
import org.testng.annotations.Test
import java.time.ZoneId
import java.time.format.DateTimeFormatter

abstract class TestSimpleFilter<C> {

    val table = "filter_tests"
    var context : IQueryGeneratorContext = refreshContext()
    abstract val testingServer: TestingServer<C>

    open fun castToDouble(value: String): String {
        return "CAST($value as ${testingServer.bridge.mqlTypeMap[StandardTypes.DOUBLE]})"
    }

    @BeforeTest
    fun refreshContext(): IQueryGeneratorContext {
        context = QueryGeneratorContext(
            ProjectAuth.singleProject(timezone = ZoneId.of("UTC")), testingServer.dataSource,
            SampleDataset.datasetService,
            JinjaRendererService(), null, null, null
        )
        return context
    }

    @Test
    fun testAnyFiltersIsSet() {
        @Language("JSON5")
        val filter = """{"dimension": "test_int", "operator": "is_set"}"""
        check("test_int", filter, listOf(SampleDataset.testString[0]))
    }

    @Test
    fun testAnyFiltersIsNotSet() {
        @Language("JSON5")
        val filter = """{"dimension": "test_int", "operator": "is_not_set"}"""
        check("test_int", filter, null)
    }

    @Test
    fun testStringFiltersEquals() {
        @Language("JSON5")
        val filter = """{"dimension": "test_string", "operator": "equals", "value": "alpha"}"""
        check("test_string", filter, listOf("alpha"))
    }
    @Test
    fun testStringFiltersNotEquals() {
        @Language("JSON5")
        val filter = """{"and":  [{"dimension": "test_string", "operator": "not_equals", "value": "alpha"}, {"dimension": "test_string", "operator": "equals", "value": "bravo"}]}"""
        check("test_string", filter, listOf("bravo"))
    }

    @Test
    fun testStringFiltersContains() {
        @Language("JSON5")
        val filter = """{"dimension": "test_string", "operator": "contains", "value": "liet"}"""
        check("test_string", filter, listOf("juliett"))
    }

    @Test
    fun testStringFiltersEndsWith() {
        @Language("JSON5")
        val filter = """{"dimension": "test_string", "operator": "ends_with", "value": "trot"}"""
        check("test_string", filter, listOf("foxtrot"))
    }

    @Test
    fun testStringFiltersStartsWith() {
        @Language("JSON5")
        val filter = """{"dimension": "test_string", "operator": "starts_with", "value": "charli"}"""
        check("test_string", filter, listOf("charlie"))
    }

    @Test
    fun testStringFiltersIn() {
        @Language("JSON5")
        val filter = """{"dimension": "test_string", "operator": "in", "value": ["alpha"]}"""
        check("test_string", filter, listOf("alpha"))
    }

    @Test
    fun testBooleanFiltersEquals() {
        @Language("JSON5")
        val filter = """{"dimension": "test_bool", "operator": "equals", "value": true}"""
        check("test_double", filter, listOf(9.0))
    }

    @Test
    fun testBooleanFiltersNotEquals() {
        @Language("JSON5")
        val filter = """{"dimension": "test_bool", "operator": "not_equals", "value": true}"""
        check("test_string", filter, listOf(9.0))
    }

    @Test
    fun testNumberFiltersEqualsInt() {
        @Language("JSON5")
        val filter = """{"dimension": "test_double", "operator": "equals", "value": 1}"""
        check(castToDouble("test_double"), filter, listOf(1.0))
    }

    @Test
    fun testNumberFiltersEqualsDouble() {
        @Language("JSON5")
        val filter = """{"dimension": "test_double", "operator": "equals", "value": "1.0"}"""
        check(castToDouble("test_double"), filter, listOf(1.0))
    }

    @Test
    fun testNumberFiltersGreaterThanInt() {
        @Language("JSON5")
        val filter = """{"dimension": "test_int", "operator": "greater_than", "value": 0}"""
        check(castToDouble("test_int"), filter, listOf(1.0))
    }

    @Test
    fun testNumberFiltersGreaterThanDouble() {
        @Language("JSON5")
        val filter = """{"dimension": "test_double", "operator": "greater_than", "value": 0}"""
        check(castToDouble("test_double"), filter, listOf(1.0))
    }

    @Test
    fun testNumberFiltersLessThanInt() {
        @Language("JSON5")
        val filter = """{"dimension": "test_int", "operator": "less_than", "value": 1}"""
        check(castToDouble("test_double"), filter, listOf(0.0))
    }

    @Test
    fun testNumberFiltersLessThanDouble() {
        @Language("JSON5")
        val filter = """{"dimension": "test_double", "operator": "less_than", "value": 1}"""
        check(castToDouble("test_double"), filter, listOf(0.0))
    }

    @Test
    fun testNumberFiltersGreaterThanAndLessThanInt() {
        @Language("JSON5")
        val filter = """{"and": [{"dimension": "test_int", "operator": "greater_than", "value": 3}, {"dimension": "test_int", "operator": "less_than", "value": 5}]}"""
        check(castToDouble("test_double"), filter,  listOf(4.0))
    }

    @Test
    fun testNumberFiltersGreaterThanAndLessThanDouble() {
        @Language("JSON5")
        val filter = """{"and": [{"dimension": "test_double", "operator": "greater_than", "value": 3}, {"dimension": "test_double", "operator": "less_than", "value": 5}]}"""
        check(castToDouble("test_double"), filter, listOf(4.0))
    }

    @Test
    fun testTimestampGreaterThan() {
        @Language("JSON5")
        val filter = """{"dimension": "test_timestamp", "operator": "greater_than", "value": ${SampleDataset.testTimestamp.last().format(DateTimeFormatter.ISO_DATE_TIME)}}"""
        check("test_timestamp", filter, null)
    }

    @Test
    fun testTimestampLessThan() {
        @Language("JSON5")
        val filter = """{"dimension": "test_timestamp", "operator": "greater_than", "value": ${SampleDataset.testTimestamp[1]}}"""
        check("test_timestamp", filter, listOf(SampleDataset.testTimestamp.first()))
    }

    @Test
    fun testTimestampGreaterThanAndLessThan() {
        @Language("JSON5")
        val filter = """{"and": [{"dimension": "test_timestamp", "operator": "greater_than", "value": ${SampleDataset.testTimestamp[3]}}, {"dimension": "test_timestamp", "operator": "less_than", "value": ${SampleDataset.testTimestamp[5]}}]}"""
        check(castToDouble("test_timestamp"), filter, listOf(SampleDataset.testTimestamp[4]))
    }

    @Test
    fun testDateGreaterThan() {
        @Language("JSON5")
        val filter = """{"dimension": "test_date", "operator": "greater_than", "value": ${SampleDataset.testDate.last()}"""
        check(castToDouble("test_date"), filter, null)
    }

    @Test
    fun testDateLessThan() {
        @Language("JSON5")
        val filter = """{"dimension": "test_date", "operator": "less_than", "value": ${SampleDataset.testDate[1]}"""
        check(castToDouble("test_date"), filter, listOf(SampleDataset.testDate.first()))
    }

    @Test
    fun testDateGreaterThanAndLessThan() {
        @Language("JSON5")
        val filter = """{"and": [{"dimension": "test_date", "operator": "greater_than", "value": ${SampleDataset.testDate[3]}}, {"dimension": "test_date", "operator": "less_than", "value": ${SampleDataset.testDate[5]}}]}"""
        check(castToDouble("test_date"), filter, listOf(SampleDataset.testDate[4]))
    }

    private fun check(column : String, filter : String, result : List<Any?>?) {
        val filter = JsonHelper.read(filter, ReportFilter::class.java)
        val generatedFilter = testingServer.bridge.renderFilter(filter, table, context)
        val query = "SELECT $column FROM ${testingServer.bridge.quoteIdentifier(table)} AS " +
            "${context.getOrGenerateAlias(table, null)} WHERE $generatedFilter"
        assertEquals(result, testingServer.runQueryFirstRow(query))
    }


}
