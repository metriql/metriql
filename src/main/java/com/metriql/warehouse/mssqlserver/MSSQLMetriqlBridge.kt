package com.metriql.warehouse.mssqlserver

import com.metriql.report.funnel.FunnelReportType
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.bridge.ANSISQLMetriqlBridge
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.funnel.ANSISQLFunnelQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator
import com.metriql.warehouse.spi.services.segmentation.Segmentation

object MSSQLMetriqlBridge : ANSISQLMetriqlBridge() {
    override val filters = MSSQLFilters()

    override val timeframes = MSSQLTimeframes()

    override val functions = super.functions + mapOf(
        RFunction.DATE_ADD to "DATEADD({{value[1]}}, {{value[2]}}, {{value[0]}})",
    )

    override val queryGenerators = mapOf(
        SegmentationReportType.slug to object : ANSISQLSegmentationQueryGenerator() {
            override fun getMap(context: IQueryGeneratorContext, queryDSL: Segmentation): Map<String, Any?> {
                // MsSQL doesn't support GROUP BY 1,2,3
                return super.getMap(context, queryDSL) + mapOf("groups" to queryDSL.groups) + if (queryDSL.limit != null) {
                    mapOf("limit" to null)
                } else {
                    mapOf()
                }
            }
        },
        FunnelReportType.slug to ANSISQLFunnelQueryGenerator()
    )

    override val supportedDBTTypes = setOf<DBTType>()
}
