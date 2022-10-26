package com.metriql.tests

import com.google.common.cache.CacheBuilderSpec
import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.report.SqlQueryTaskGenerator
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.data.ReportMetric
import com.metriql.report.segmentation.SegmentationQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.report.sql.SqlQuery
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.cache.InMemoryCacheService
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Dataset
import com.metriql.service.model.Dataset.Dimension.DimensionValue.Column
import com.metriql.service.model.Dataset.Dimension.Type.COLUMN
import com.metriql.service.model.Dataset.MappingDimensions.CommonMappings.TIME_SERIES
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import org.intellij.lang.annotations.Language
import org.testng.Assert.assertEquals
import org.testng.Assert.assertNotNull
import org.testng.Assert.assertNull
import org.testng.Assert.assertTrue
import org.testng.Assert.fail
import org.testng.annotations.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset

abstract class TestSegmentation {
    abstract val dataSource: DataSource
    private val sqlQueryTaskGenerator = SqlQueryTaskGenerator(InMemoryCacheService(CacheBuilderSpec.disableCaching()))
    private val auth: ProjectAuth = ProjectAuth(
        1, "",
        isOwner = true, isSuperuser = true, email = null, permissions = null, attributes = null, timezone = ZoneId.of("UTC"), source = null
    )
    private val rendererService = JinjaRendererService()

    private val datasetService = TestDatasetService(getModels())
    private val service: SegmentationService get() = SegmentationService()

    private fun generateReportFilter(dateRange: DateRange): ReportFilter {
        return ReportFilter(
            ReportFilter.Type.METRIC,
            MetricFilter(
                MetricFilter.Connector.AND,
                listOf(
                    MetricFilter.Filter(
                        MetricFilter.MetricType.MAPPING_DIMENSION,
                        ReportMetric.ReportMappingDimension(TIME_SERIES, null), TimestampOperatorType.BETWEEN.name,
                        mapOf("start" to dateRange.start.toString(), "end" to dateRange.end.toString())
                    )
                )
            )
        )
    }

    @Test
    fun testTimestampPostProcessorHour() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::hour"]
          }""",
            SegmentationQuery::class.java
        )
        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))

        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals(test.result!!.first()[0], LocalDate.of(1970, 4, 9).atStartOfDay())
    }

    private fun getContext(): IQueryGeneratorContext {
        return QueryGeneratorContext(
            auth,
            dataSource,
            datasetService,
            rendererService,
            reportExecutor = null,
            userAttributeFetcher = null,
            dependencyFetcher = null
        )
    }

    private fun execute(context: IQueryGeneratorContext, query: String): QueryResult {
        val task = sqlQueryTaskGenerator.createTask(
            auth,
            context,
            context.datasource,
            query,
            SqlQuery.QueryOptions(WarehouseQueryTask.DEFAULT_LIMIT, null, null, true),
            false
        )
        return task.runAndWaitForResult()
    }

    @Test
    fun testTimestampPostProcessorDay() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::day"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0]), LocalDate.ofEpochDay(98))
    }

    @Test
    fun testTimestampPostProcessorWeek() {

        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::week"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0]), LocalDate.of(1970, 4, 6))
    }

    @Test
    fun testTimestampPostProcessorMonth() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::month"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0]), LocalDate.of(1970, 4, 1))
    }

    @Test
    fun testTimestampPostProcessorYear() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::year"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0]), LocalDate.of(1970, 1, 1))
    }

    @Test
    fun testTimestampPostProcessorHourOfDay() {

        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::hour_of_day"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0]), LocalTime.of(0, 0))
    }

    @Test
    fun testTimestampPostProcessorDayOfMonth() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::day_of_month"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0] as Number).toLong(), 9L)
    }

    @Test
    fun testTimestampPostProcessorWeekOfYear() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::week_of_year"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0] as Number).toLong(), 15.toLong())
    }

    @Test
    fun testTimestampPostProcessorMonthOfYear() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::month_of_year"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertTrue(listOf("April", "Apr", "4th Month").contains((test.result!!.first()[0].toString())))
    }

    @Test
    fun testTimestampPostProcessorQuarterOfYear() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::quarter_of_year"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0] as String), "Q2")
    }

    @Test
    fun testTimestampPostProcessorDayOfWeek() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time::day_of_week"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertTrue(listOf("Thursday").contains((test.result!!.first()[0].toString())))
    }

    @Test
    fun testMultipleMeasures() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["testnumber", "testbool"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertNotNull(test.result, "Test result can't be null.")
    }

    @Test
    fun testNonExistsDimension() {

        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["testnumber", "testnumber_doesntexist"]
          }""",
            SegmentationQuery::class.java
        )
        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99)))
        val context = getContext()
        execute(
            context,
            try {
                service.renderQuery(
                    auth,
                    context,
                    report,
                    reportFilters,
                ).query
            } catch (e: MetriqlException) {
                assertTrue(e.statusCode == HttpResponseStatus.NOT_FOUND)
                return
            }
        )

        fail("Request should fail")
    }

    @Test
    fun testNumericDimension() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["testnumber"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNotNull(test.result)
        val numberDimensions = test.result!!.map { it.first() as Number }.sortedBy { it.toInt() }
        assertEquals(numberDimensions.first(), 0.0)
        assertEquals(numberDimensions.last(), 98.0)
    }

    @Test
    fun testStringDimension() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["teststr"]
          }""",
            SegmentationQuery::class.java
        )
        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNotNull(test.result)
        val stringDimensions = test.result!!.map { it.first().toString() }.sorted()
        assertEquals(stringDimensions.first(), "test0")
    }

    @Test
    fun testTimestampDimension() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["_time"]
          }""", SegmentationQuery::class.java
        )
        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNotNull(test.result)
        val timestampDimensions = test.result!!.map { it.first() as LocalDateTime }.sorted()
        assertEquals(timestampDimensions.first(), LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC))
        assertEquals(timestampDimensions.last(), LocalDateTime.ofEpochSecond(60 * 60 * 24 * 98, 0, ZoneOffset.UTC))
    }

    @Test
    fun testDateDimension() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["testdate"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNotNull(test.result)
        assertEquals(test.result!!.first().first().toString(), "1970-01-01")
        assertEquals((test.result!!.first()[1] as Number).toLong(), (99).toLong())
    }

    @Test
    fun testTotalStatistics() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"]
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertTrue(test.error == null, if (test.error != null) test.error!!.message else null)
        assertEquals((test.result!!.first()[0] as Number).toLong(), 99L)
    }

    @Test
    fun testAllDimensionsNumberBoolean() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "dimensions": ["testnumber", "testbool"]
          }""",
            SegmentationQuery::class.java
        )
        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
        assertNotNull(test.result, "Test result can't be null.")
    }

    @Test
    fun testSegmentationWithFilter() {
        @Language("JSON5")
        val report = JsonHelper.read(
            """{
            "dataset":  "_table", 
            "measures": ["measure"],
            "filters": {"type": "metric", "value": {"connector": "and", "filters": [{"metric": "testnumber", "operator":  "greater_than", "value":  10}]}}
          }""",
            SegmentationQuery::class.java
        )

        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99)))
        val context = getContext()
        val test = execute(
            context,
            service.renderQuery(
                auth,
                context,
                report,
                reportFilters,
            ).query
        )

        assertEquals((test.result!!.first()[0] as Number).toLong(), 88L)
    }

    /*@Test
    fun testJoinDimension() {
        val dataSet = Dataset("_table", SimpleFilter(listOf(
            SimpleFilterItem(
                "_table2",
                "testnumber",
                FieldType.INTEGER,
                Connector.AND,
                NumberOperatorType.GREATER_THAN,
                0
            )
        )))
        val measure = ReportMeasure("measure", "_table", null)
        ReportDimension("teststr", "_table2", null)
        val dimensions = listOf(Dimension(COLUMN, ColumnValue(DataMappingHttpService.RakamCollection("_table2", TableType.EVENT), "teststr", null), false))
        val dimensions = listOf(ReportDimension("testdate", "_table", null))
        val report = SegmentationReportOptions(dataSet, dimensions, listOf(measure), null)
        val reportFilters = generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
        val test = service.queryTask(
            auth,
            null,
            report,
            reportFilters,,
                isBackgroundTask = true


        ).waitForResult()

        assertNull(test.error, if (test.error != null) test.error!!.message else null)
    }*/

    companion object {
        const val SCALE_FACTOR = 100

        fun getModels(): List<Dataset> {
            val dimensions = listOf(
                Dataset.Dimension("teststr", COLUMN, Column("teststr"), fieldType = FieldType.STRING),
                Dataset.Dimension("testnumber", COLUMN, Column("testnumber"), fieldType = FieldType.DOUBLE),
                Dataset.Dimension("testbool", COLUMN, Column("testbool"), fieldType = FieldType.BOOLEAN),
                Dataset.Dimension("testarray", COLUMN, Column("testarray"), fieldType = FieldType.ARRAY_STRING),
                Dataset.Dimension("testdate", COLUMN, Column("testdate"), fieldType = FieldType.DATE),
                Dataset.Dimension("_time", COLUMN, Column("_time"), fieldType = FieldType.TIMESTAMP),
            )

            val dataset1 = Dataset(
                "_table", false,
                Dataset.Target.initWithTable(null, "rakam_test", "_table"),
                null, null, null, Dataset.MappingDimensions.build(TIME_SERIES to "_time"), listOf(),
                dimensions,
                listOf(Dataset.Measure("measure", null, null, null, Dataset.Measure.Type.COLUMN, Dataset.Measure.MeasureValue.Column(Dataset.Measure.AggregationType.COUNT, null))),
            )

            val dataset2 = dataset1.copy(
                name = "_table2",
                target = Dataset.Target.initWithTable(null, "rakam_test", "_table2"),
                dimensions = dataset1.dimensions + listOf(Dataset.Dimension("testdummy", COLUMN, Column("testdummy"), fieldType = FieldType.STRING))
            )

            return listOf(dataset1, dataset2)
        }
    }
}
