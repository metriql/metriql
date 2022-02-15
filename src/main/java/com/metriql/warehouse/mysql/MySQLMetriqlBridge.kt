package com.metriql.warehouse.mysql

import com.metriql.report.funnel.FunnelReportType
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator

object MySQLMetriqlBridge : ANSISQLMetriqlBridge() {

    override val filters = BaseMySQLFilters { MySQLMetriqlBridge }
    override val timeframes = MySQLTimeframes()
    override val queryGenerators = mapOf(
        SegmentationReportType.slug to ANSISQLSegmentationQueryGenerator(),
        FunnelReportType.slug to ANSISQLFunnelQueryGenerator()
    )

    override val quote = '`'
    override val supportedDBTTypes = setOf<DBTType>()

    override val functions = super.functions + mapOf(
        RFunction.DATE_ADD to "DATE_ADD({{value[0]}}, INTERVAL {{value[2]}} {{value[1]}})'",
        RFunction.HEX_TO_INT to "CONV({{value[0]}}, 16, 10)",
    )
}
