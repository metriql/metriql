package com.metriql.warehouse.presto

import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
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

    override val aliasQuote = '"'
    override val supportedDBTTypes = setOf(DBTType.TABLE, DBTType.VIEW)
}
