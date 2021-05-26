package com.metriql.tests

import com.google.common.cache.CacheBuilderSpec
import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.db.TestingServer
import com.metriql.interfaces.TestModelService
import com.metriql.report.ReportFilter
import com.metriql.report.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.ReportMetric
import com.metriql.report.ReportMetric.PostOperation
import com.metriql.report.ReportMetric.ReportDimension
import com.metriql.report.ReportMetric.ReportMeasure
import com.metriql.report.SqlQueryTaskGenerator
import com.metriql.report.segmentation.SegmentationReportOptions
import com.metriql.report.segmentation.SegmentationService
import com.metriql.report.sql.SqlReportOptions
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.cache.InMemoryCacheService
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Model
import com.metriql.service.model.Model.Dimension.DimensionValue.Column
import com.metriql.service.model.Model.Dimension.Type.COLUMN
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP
import com.metriql.util.MetriqlException
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.filter.NumberOperatorType
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
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
    abstract val testingServer: TestingServer<*, *>
    private val auth: ProjectAuth = ProjectAuth(
        1, 1,
        isOwner = true, isSuperuser = true, email = null, permissions = null, timezone = ZoneId.of("UTC")
    )
    private val rendererService = JinjaRendererService()

    private val modelService = TestModelService(getModels())
    private val service: SegmentationService get() = SegmentationService()

    private fun generateReportFilter(dateRange: DateRange): ReportFilter {
        return ReportFilter(
            ReportFilter.Type.METRIC_FILTER,
            MetricFilter(
                MetricFilter.MetricType.MAPPING_DIMENSION,
                ReportMetric.ReportMappingDimension(EVENT_TIMESTAMP, null),
                listOf(MetricFilter.Filter(FieldType.TIMESTAMP, TimestampOperatorType.BETWEEN, mapOf("start" to dateRange.start.toString(), "end" to dateRange.end.toString())))
            )
        )
    }

    @Test
    fun testTimestampPostProcessorHour() {
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time",
                "_table",
                null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.HOUR
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))

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
            modelService,
            rendererService,
            reportExecutor = null,
            userAttributeFetcher = null,
        )
    }

    private fun execute(context: IQueryGeneratorContext, query: String): QueryResult {
        val task = sqlQueryTaskGenerator.createTask(
            auth,
            context,
            context.datasource,
            query,
            SqlReportOptions.QueryOptions(WarehouseQueryTask.DEFAULT_LIMIT, null, null, true),
            false
        )
        return task.runAndWaitForResult()
    }

    @Test
    fun testTimestampPostProcessorDay() {
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.DAY
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.WEEK
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.MONTH
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.YEAR
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.HOUR_OF_DAY
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.DAY_OF_MONTH
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.WEEK_OF_YEAR
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.MONTH_OF_YEAR
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.QUARTER_OF_YEAR
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension(
                "_time", "_table", null,
                PostOperation(
                    PostOperation.Type.TIMESTAMP,
                    TimestampPostOperation.DAY_OF_WEEK
                )
            )
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension("testnumber", "_table", null, null),
            ReportDimension("testbool", "_table", null, null)
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension("testnumber", "_table", null, null),
            ReportDimension("testnumber_doesntexist", "_table", null, null)
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(98), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(ReportDimension("testnumber", "_table", null, null))

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(ReportDimension("teststr", "_table", null, null))

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(ReportDimension("_time", "_table", null, null))

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(ReportDimension("testdate", "_table", null, null))

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf<ReportDimension>()

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf(
            ReportDimension("testnumber", "_table", null, null),
            ReportDimension("testbool", "_table", null, null)
        )

        val report = SegmentationReportOptions("_table", dimensions, listOf(measure))
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
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
        val measure = ReportMeasure("_table", "measure", null)
        val dimensions = listOf<ReportDimension>()

        val filter = ReportFilter(
            ReportFilter.Type.METRIC_FILTER,
            MetricFilter(
                MetricFilter.MetricType.DIMENSION,
                ReportDimension("testnumber", "_table", null, null),
                listOf(MetricFilter.Filter(FieldType.INTEGER, NumberOperatorType.GREATER_THAN, 10))
            )
        )
        val report = SegmentationReportOptions(
            "_table",
            dimensions,
            listOf(measure),
            filters = listOf(filter),
            reportOptions = null
        )
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
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
        val reportFilters = listOf(generateReportFilter(DateRange(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(99))))
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

        fun getModels(): List<Model> {
            val dimensions = listOf(
                Model.Dimension("teststr", COLUMN, Column("teststr"), fieldType = FieldType.STRING),
                Model.Dimension("testnumber", COLUMN, Column("testnumber"), fieldType = FieldType.DOUBLE),
                Model.Dimension("testbool", COLUMN, Column("testbool"), fieldType = FieldType.BOOLEAN),
                Model.Dimension("testarray", COLUMN, Column("testarray"), fieldType = FieldType.ARRAY_STRING),
                Model.Dimension("testdate", COLUMN, Column("testdate"), fieldType = FieldType.DATE),
                Model.Dimension("_time", COLUMN, Column("_time"), fieldType = FieldType.TIMESTAMP),
            )

            val model1 = Model(
                "_table", false,
                Model.Target.initWithTable(null, "rakam_test", "_table"),
                null, null, null, Model.MappingDimensions.build(EVENT_TIMESTAMP to "_time"), listOf(),
                dimensions,
                listOf(Model.Measure("measure", null, null, null, Model.Measure.Type.COLUMN, Model.Measure.MeasureValue.Column(Model.Measure.AggregationType.COUNT, null))),
            )

            val model2 = model1.copy(
                name = "_table2",
                target = Model.Target.initWithTable(null, "rakam_test", "_table2"),
                dimensions = model1.dimensions + listOf(Model.Dimension("testdummy", COLUMN, Column("testdummy"), fieldType = FieldType.STRING))
            )

            return listOf(model1, model2)
        }
    }
}
