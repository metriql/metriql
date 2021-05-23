package com.metriql.warehouse.mysql

import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.services.ServiceType
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator

object MySQLMetriqlBridge : ANSISQLMetriqlBridge() {

    override val filters = BaseMySQLFilters { MySQLMetriqlBridge }
    override val timeframes = MySQLTimeframes()
    override val queryGenerators = mapOf(
        ServiceType.SEGMENTATION to ANSISQLSegmentationQueryGenerator(),
        ServiceType.FUNNEL to ANSISQLFunnelQueryGenerator()
    )

    override val aliasQuote = '`'
    override val supportedDBTTypes = setOf<DBTType>()

    override val functions = mapOf(
        RFunction.NOW to "CURRENT_TIMESTAMP",
        RFunction.DATE_ADD to "DATE_ADD({{value[0]}}, INTERVAL {{value[2]}} {{value[1]}})'",
        RFunction.HEX_TO_INT to "CONV({{value[0]}}, 16, 10)",
    )
}
