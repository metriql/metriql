package com.metriql.warehouse.spi.services.segmentation

import com.metriql.report.segmentation.SegmentationReportOptions
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceSupport

open class ANSISQLSegmentationQueryGenerator : SegmentationQueryGenerator {
    override fun supports() = listOf<ServiceSupport>()

    open fun getMap(context: IQueryGeneratorContext, queryDSL: Segmentation): Map<String, Any?> {
        return mapOf(
            "projections" to queryDSL.projections,
            "tableReference" to queryDSL.tableReference,
            "joins" to queryDSL.joins,
            "whereFilters" to queryDSL.whereFilters,
            "groups" to queryDSL.groupIdx,
            "havingFilters" to queryDSL.havingFilters,
            "orderBys" to queryDSL.orderBy,
            /*
            * If query includes view models (WITH view as ().., ne should discard the WITH prefix while rendering the query),
            * since view models are appended to query as WITH view as (), we have to only add ","
            * otherwise start with WITH keyword
            * {% if has_view_models %}, {% else %}WITH {% endif %} first_action AS (
            * */
            "has_view_models" to context.viewModels.isNotEmpty()
        )
    }

    override fun generateSQL(auth: ProjectAuth, context: IQueryGeneratorContext, queryDSL: Segmentation, options: SegmentationReportOptions): String {
        return jinja.render(standard, getMap(context, queryDSL))
    }

    companion object {
        val standard = this::class.java.getResource("/sql/segmentation/warehouse/ansi/standard.jinja2").readText()
    }
}
