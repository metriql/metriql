package com.metriql.warehouse.postgresql

import com.metriql.db.FieldType.TIMESTAMP
import com.metriql.report.data.ReportMetric
import com.metriql.report.flow.FlowReportType
import com.metriql.report.funnel.FunnelReportType
import com.metriql.report.retention.RetentionReportType
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.service.model.Model
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.MetricPositionType.PROJECTION
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.flow.ANSISQLFlowQueryGenerator
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator

open abstract class BasePostgresqlMetriqlBridge : ANSISQLMetriqlBridge() {
    override val supportedDBTTypes = setOf(DBTType.INCREMENTAL, DBTType.TABLE, DBTType.VIEW)

    override val functions = super.functions + mapOf(
        RFunction.DATE_ADD to "{{value[0]}} + INTERVAL '{{value[2]}} {{value[1]}}'",
//        RFunction.HEX_TO_INT to "CAST(CAST(({{value[0]}} || '00000100') AS bit(32)) AS bigint)",
        RFunction.HEX_TO_INT to "('x' || lpad({{value[0]}}, 16, '0'))::bit(64)::bigint",
    )

    override val metricRenderHook: WarehouseMetriqlBridge.MetricRenderHook
        get() = object : WarehouseMetriqlBridge.MetricRenderHook {
            override fun dimensionBeforePostOperation(
                context: IQueryGeneratorContext,
                metricPositionType: WarehouseMetriqlBridge.MetricPositionType,
                dimension: Model.Dimension,
                postOperation: ReportMetric.PostOperation?,
                dimensionValue: String,
            ): String {
                val zoneId = context.auth.timezone
                return if (metricPositionType == PROJECTION && zoneId != null && dimension.fieldType == TIMESTAMP) {
                    "TIMEZONE('$zoneId', $dimensionValue)"
                } else {
                    dimensionValue
                }
            }
        }
}

object PostgresqlMetriqlBridge : BasePostgresqlMetriqlBridge() {
    override val filters = PostgresqlFilters { PostgresqlMetriqlBridge }
    override val timeframes = PostgresqlTimeframes()

    override val queryGenerators = mapOf(
        SegmentationReportType.slug to ANSISQLSegmentationQueryGenerator(),
        FunnelReportType.slug to ANSISQLFunnelQueryGenerator(),
        RetentionReportType.slug to PostgresqlRetentionQueryGenerator(),
        FlowReportType.slug to ANSISQLFlowQueryGenerator(),
    )
}
