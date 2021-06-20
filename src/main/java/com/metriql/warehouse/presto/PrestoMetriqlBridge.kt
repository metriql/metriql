package com.metriql.warehouse.presto

import com.metriql.service.model.Model
import com.metriql.service.model.Model.Measure.AggregationType.APPROXIMATE_UNIQUE
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.ADHOC
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_ACCUMULATE
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge.AggregationContext.INTERMEDIATE_MERGE
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.services.ServiceType
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator

object PrestoMetriqlBridge : ANSISQLMetriqlBridge() {
    override val filters = PrestoFilters { PrestoMetriqlBridge }
    override val timeframes = PrestoTimeframes()
    override val queryGenerators = mapOf(
        ServiceType.SEGMENTATION to ANSISQLSegmentationQueryGenerator(),
        ServiceType.FUNNEL to ANSISQLFunnelQueryGenerator()
    )

    override val functions = mapOf(
        RFunction.NOW to "CURRENT_TIMESTAMP",
        RFunction.DATE_ADD to "{{value[0]}} + interval '{{value[2]}}' {{value[1]}}",
    )

    override val supportedDBTTypes = setOf(DBTType.TABLE, DBTType.VIEW)

    override fun performAggregation(columnValue: String, aggregationType: Model.Measure.AggregationType, context: WarehouseMetriqlBridge.AggregationContext): String {
        return if (aggregationType == APPROXIMATE_UNIQUE) {
            when (context) {
                ADHOC -> "approx_distinct($columnValue)"
                INTERMEDIATE_ACCUMULATE -> "approx_set($columnValue)"
                INTERMEDIATE_MERGE -> "cardinality(merge($columnValue))"
            }
        } else {
            super.performAggregation(columnValue, aggregationType, context)
        }
    }
}
