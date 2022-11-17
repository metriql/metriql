package com.metriql.warehouse.bigquery

import com.metriql.db.FieldType
import com.metriql.report.data.ReportMetric
import com.metriql.report.funnel.FunnelReportType
import com.metriql.report.retention.RetentionReportType
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.Dataset.Measure.AggregationType.APPROXIMATE_UNIQUE
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.ADHOC
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_ACCUMULATE
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_MERGE
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.MetricPositionType.PROJECTION
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator
import io.trino.spi.type.StandardTypes

object BigQueryMetriqlBridge : ANSISQLMetriqlBridge() {
    override val filters = BigQueryFilters { BigQueryMetriqlBridge }
    override val timeframes = BigQueryTimeframes()
    override val quote = '`'

    override val queryGenerators = mapOf(
        SegmentationReportType.slug to ANSISQLSegmentationQueryGenerator(),
        FunnelReportType.slug to ANSISQLFunnelQueryGenerator(template = BigQueryMetriqlBridge::class.java.getResource("/sql/funnel/warehouse/bigquery/generic.jinja2").readText()),
        RetentionReportType.slug to BigQueryRetentionQueryGenerator()
    )

    override val mqlTypeMap = super.mqlTypeMap + mapOf(
        StandardTypes.VARCHAR to "string",
        StandardTypes.TIMESTAMP to "datetime",
        StandardTypes.DOUBLE to "float64"
    )

    override val functions = super.functions + mapOf(
        RFunction.DATE_ADD to "DATE_ADD({{value[0]}}, INTERVAL {{value[2]}} {{value[1]}})",
        RFunction.DATE_DIFF to "DATE_DIFF({{value[0]}}, {{value[1]}}, {{value[2]}})",
        RFunction.HEX_TO_INT to "CAST(CONCAT('0x', {{value[0]}}) AS INT64)",
        RFunction.DATE_TRUNC to "DATETIME_TRUNC({{value[1]}}, {{value[0]}})",
    )

    override val metricRenderHook: WarehouseMetriqlBridge.MetricRenderHook
        get() = object : WarehouseMetriqlBridge.MetricRenderHook {
            override fun dimensionBeforePostOperation(
                context: IQueryGeneratorContext,
                metricPositionType: WarehouseMetriqlBridge.MetricPositionType,
                dimension: Dataset.Dimension,
                timeframe: ReportMetric.Timeframe?,
                dimensionValue: String,
            ): String {
                val zoneId = context.auth.timezone
                return when {
                    // https://stackoverflow.com/questions/47713945/bigquery-datetime-vs-timestamp
                    // Only do this on projection. Filters already handle timestamp conversion
                    metricPositionType == PROJECTION && dimension.fieldType == FieldType.TIMESTAMP && zoneId != null -> {
                        "TIMESTAMP(FORMAT_TIMESTAMP('%F %T', CAST($dimensionValue AS TIMESTAMP) , '$zoneId'))"
                    }
                    dimension.fieldType == FieldType.TIMESTAMP && zoneId != null -> {
                        "CAST($dimensionValue as TIMESTAMP)"
                    }
                    else -> dimensionValue
                }
            }
        }
    override val supportedDBTTypes = setOf(DBTType.INCREMENTAL, DBTType.TABLE, DBTType.VIEW)

    override fun quoteIdentifier(identifier: String): String {
        return super.quoteIdentifier(
            identifier
                .replace(".", "__")
                .replace("::", "___")
        )
    }

    override fun performAggregation(columnValue: String, aggregationType: Dataset.Measure.AggregationType, context: WarehouseMetriqlBridge.AggregationContext): String {
        return if (aggregationType == APPROXIMATE_UNIQUE) {
            when (context) {
                ADHOC -> "APPROX_COUNT_DISTINCT($columnValue)"
                // this function only accepts string
                INTERMEDIATE_ACCUMULATE -> "HLL_COUNT.INIT(CAST($columnValue AS STRING))"
                INTERMEDIATE_MERGE -> "HLL_COUNT.MERGE(CAST($columnValue AS STRING))"
            }
        } else {
            super.performAggregation(columnValue, aggregationType, context)
        }
    }
}
