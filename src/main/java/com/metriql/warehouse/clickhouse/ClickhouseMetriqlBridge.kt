package com.metriql.warehouse.clickhouse

import com.metriql.db.FieldType
import com.metriql.report.data.ReportMetric
import com.metriql.report.funnel.FunnelReportType
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.service.model.Model
import com.metriql.util.DefaultJinja
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.Segmentation
import io.trino.spi.type.StandardTypes

object ClickhouseMetriqlBridge : ANSISQLMetriqlBridge() {
    override val filters = ClickhouseFilters { ClickhouseMetriqlBridge }
    override val timeframes = ClickhouseTimeframes()
    override val queryGenerators = mapOf(
        SegmentationReportType.slug to object : ANSISQLSegmentationQueryGenerator() {
            override fun getMap(context: IQueryGeneratorContext, queryDSL: Segmentation): Map<String, Any?> {
                return super.getMap(context, queryDSL) + mapOf("groups" to queryDSL.groups, "orderBy" to queryDSL.orderBy)
            }
        },
        FunnelReportType.slug to ANSISQLFunnelQueryGenerator()
    )

    override val mqlTypeMap = super.mqlTypeMap + mapOf(
        StandardTypes.TIME to "varchar"
    )

    override val functions = super.functions + mapOf(
        RFunction.NOW to "NOW()",
        RFunction.DATE_ADD to "date_add({{value[1]}}, {{value[2]}}, {{value[0]}})",
        RFunction.DATE_DIFF to "date_diff({{value[2]}}, {{value[0]}}, {{value[0]})",
//        RFunction.HEX_TO_INT to "reinterpretAsInt64(reverse(unhex({{value[0]}})))"
        RFunction.HEX_TO_INT to "{{value[0]}}"
    )

    override val supportedDBTTypes = setOf(DBTType.TABLE, DBTType.VIEW)

    override fun performAggregation(columnValue: String, aggregationType: Model.Measure.AggregationType, context: WarehouseMetriqlBridge.AggregationContext): String {
        return if (aggregationType == Model.Measure.AggregationType.APPROXIMATE_UNIQUE) {
            when (context) {
                WarehouseMetriqlBridge.AggregationContext.ADHOC -> "uniq($columnValue)"
                WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_ACCUMULATE -> "uniqHLL12($columnValue)"
                WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_MERGE -> "uniqCombined64(uniqCombined($columnValue))"
            }
        } else {
            super.performAggregation(columnValue, aggregationType, context)
        }
    }

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
                // perform timezone conversion on both filter and projection because GROUP BY expressions don't match otherwise and
                // Clickhouse is not sophisticated enough to understand the uniq
                // Only convert timezone if no post operation is present. Snowflake does not accept post operation on converted timezone
                // A possible bug: SQL execution internal error: Processing aborted due to error 370001:653186283; incident 2921766.
                dimension.fieldType == FieldType.TIMESTAMP &&
                    zoneId != null -> {
                    "toTimezone($dimensionValue, '$zoneId')"
                }
                else -> dimensionValue
            }
        }
    }

    override fun nonSymmetricAggregateSum(sql: String, primaryKey: String): String? {
        val functionDefinition = functions[RFunction.HEX_TO_INT] ?: return null
        val uniqueIdForRow = DefaultJinja.renderFunction(functionDefinition, listOf(primaryKey))
        return """(SUM(DISTINCT $uniqueIdForRow + $sql) - SUM(DISTINCT $uniqueIdForRow))""".trimMargin()
    }
}
