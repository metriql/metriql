package com.metriql.warehouse.mssqlserver

import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceType
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.Segmentation

object MSSQLMetriqlBridge : ANSISQLMetriqlBridge() {
    override val filters = MSSQLFilters()

    override val timeframes = MSSQLTimeframes()

    override val queryGenerators = mapOf(
        ServiceType.SEGMENTATION to object : ANSISQLSegmentationQueryGenerator() {
            override fun getMap(context: IQueryGeneratorContext, queryDSL: Segmentation): Map<String, Any?> {
                // MsSQL doesn't support GROUP BY 1,2,3
                return super.getMap(context, queryDSL) + mapOf("groups" to queryDSL.groups)
            }
        },
        ServiceType.FUNNEL to ANSISQLFunnelQueryGenerator()
    )

    override val functions = mapOf(
        RFunction.NOW to "CURRENT_TIMESTAMP",
        RFunction.DATE_ADD to "DATEADD({{value[1]}}, {{value[2]}}, {{value[0]}})",
    )

    override val aliasQuote = '"'
    override val supportedDBTTypes = setOf<DBTType>()
}
