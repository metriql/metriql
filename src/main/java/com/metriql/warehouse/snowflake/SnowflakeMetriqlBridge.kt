package com.metriql.warehouse.snowflake

import com.metriql.db.FieldType
import com.metriql.report.ReportMetric
import com.metriql.service.model.Model
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.ADHOC
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_ACCUMULATE
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_MERGE
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.MetricPositionType.PROJECTION
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceType
import com.metriql.warehouse.spi.services.flow.ANSISQLFlowQueryGenerator
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator

object SnowflakeMetriqlBridge : ANSISQLMetriqlBridge() {
    private val identifierRegex = "^[A-Za-z0-9_]+$".toRegex()

    override val filters = SnowflakeFilters { SnowflakeMetriqlBridge }
    override val timeframes = SnowflakeTimeframes()

    // it's not case-sensitive
    override fun quoteIdentifier(identifier: String): String {
        return if (identifierRegex.matches(identifier)) {
            identifier
        } else {
            super.quoteIdentifier(identifier)
        }
    }

    override val queryGenerators = mapOf(
        ServiceType.SEGMENTATION to ANSISQLSegmentationQueryGenerator(),
        ServiceType.FUNNEL to ANSISQLFunnelQueryGenerator(template = SnowflakeMetriqlBridge::class.java.getResource("/sql/funnel/warehouse/snowflake/generic.jinja2").readText()),
        ServiceType.RETENTION to SnowflakeRetentionQueryGenerator(),
        ServiceType.FLOW to ANSISQLFlowQueryGenerator(),
    )

    override val functions = mapOf(
        RFunction.NOW to "CURRENT_TIMESTAMP",
        RFunction.DATE_ADD to "DATEADD({{value[1]}}, {{value[2]}}, {{value[0]}})",
        RFunction.HEX_TO_INT to "TO_NUMBER({{value[0]}}, 'XXXXXXXXXXXXXXXX')",
    )

    override val supportedDBTTypes = setOf(DBTType.INCREMENTAL, DBTType.TABLE, DBTType.VIEW)
    override val metricRenderHook = object : WarehouseMetriqlBridge.MetricRenderHook {
        override fun dimensionBeforePostOperation(
            context: IQueryGeneratorContext,
            metricPositionType: WarehouseMetriqlBridge.MetricPositionType,
            dimension: Model.Dimension,
            postOperation: ReportMetric.PostOperation?,
            dimensionValue: String,
        ): String {
            val zoneId = context.auth.timezone
            return when {
                // Only convert timezone if no post operation is present. Snowflake does not accept post operation on converted timezone
                // A possible bug: SQL execution internal error: Processing aborted due to error 370001:653186283; incident 2921766.
                metricPositionType == PROJECTION && dimension.fieldType == FieldType.TIMESTAMP &&
                    zoneId != null -> {
                    "CONVERT_TIMEZONE('$zoneId', $dimensionValue)"
                }
                else -> dimensionValue
            }
        }
    }

    override fun performAggregation(columnValue: String, aggregationType: Model.Measure.AggregationType, context: WarehouseMetriqlBridge.AggregationContext): String {
        return if (aggregationType == Model.Measure.AggregationType.APPROXIMATE_UNIQUE) {
            when (context) {
                ADHOC -> "approx_count_distinct($columnValue)"
                INTERMEDIATE_ACCUMULATE -> "hll_accumulate($columnValue)"
                INTERMEDIATE_MERGE -> "hll_estimate(hll_combine($columnValue))"
            }
        } else {
            super.performAggregation(columnValue, aggregationType, context)
        }
    }
}
