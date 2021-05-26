package com.metriql.warehouse.redshift

import com.metriql.service.model.Model
import com.metriql.warehouse.postgresql.BasePostgresqlMetriqlBridge
import com.metriql.warehouse.postgresql.PostgresqlFilters
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.services.ServiceType
import com.metriql.warehouse.spi.services.flow.ANSISQLFlowQueryGenerator
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator

object RedshiftMetriqlBridge : BasePostgresqlMetriqlBridge() {

    override val filters = PostgresqlFilters { RedshiftMetriqlBridge }
    override val timeframes = RedshiftTimeframes()
    override val queryGenerators = mapOf(
        ServiceType.SEGMENTATION to ANSISQLSegmentationQueryGenerator(),
        ServiceType.FUNNEL to ANSISQLFunnelQueryGenerator(partitionSuffix = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
        ServiceType.RETENTION to RedshiftRetentionQueryGenerator(),
        ServiceType.FLOW to ANSISQLFlowQueryGenerator(),
    )

    override val functions = super.functions + mapOf(
        RFunction.HEX_TO_INT to "STRTOL({{value[0]}}, 16)",
    )

    override val aliasQuote = '"'
    override val supportedDBTTypes = setOf(DBTType.INCREMENTAL, DBTType.TABLE, DBTType.VIEW)

    override fun performAggregation(columnValue: String, aggregationType: Model.Measure.AggregationType, context: WarehouseMetriqlBridge.AggregationContext): String {
        return (
            if (aggregationType == Model.Measure.AggregationType.APPROXIMATE_UNIQUE) {
                when (context) {
                    WarehouseMetriqlBridge.AggregationContext.ADHOC -> "APPROXIMATE COUNT($columnValue)"
                    else -> null
                }
            } else null
            ) ?: super.performAggregation(columnValue, aggregationType, context)
    }
}
