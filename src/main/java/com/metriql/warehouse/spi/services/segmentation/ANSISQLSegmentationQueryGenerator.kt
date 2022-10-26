package com.metriql.warehouse.spi.services.segmentation

import com.metriql.report.segmentation.SegmentationQuery
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceSupport

open class ANSISQLSegmentationQueryGenerator : SegmentationQueryGenerator {
    override fun supports() = listOf<ServiceSupport>()

    open fun getMap(context: IQueryGeneratorContext, queryDSL: Segmentation): Map<String, Any?> {
        val projections = (queryDSL.dimensions + queryDSL.measures).ifEmpty {
            listOf(WarehouseMetriqlBridge.RenderedField("*", null, false, null))
        }
        return mapOf(
            "projections" to projections,
            "tableReference" to queryDSL.tableReference,
            "tableAlias" to queryDSL.tableAlias,
            "joins" to queryDSL.joins,
            "limit" to (queryDSL.limit?.let { "LIMIT $it" } ?: ""),
            "whereFilter" to queryDSL.whereFilter,
            "groups" to queryDSL.groupIdx,
            "havingFilter" to queryDSL.havingFilter,
            "orderBy" to queryDSL.orderByIdx,
            /*
            * If query includes view models (WITH view as ().., ne should discard the WITH prefix while rendering the query),
            * since view models are appended to query as WITH view as (), we have to only add ","
            * otherwise start with WITH keyword
            * {% if has_view_models %}, {% else %}WITH {% endif %} first_action AS (
            * */
            "has_view_models" to context.viewModels.isNotEmpty(),
            "has_window" to (queryDSL.dimensions.any { it.window } || queryDSL.measures.any { it.window })
        )
    }

    override fun generateSQL(auth: ProjectAuth, context: IQueryGeneratorContext, queryDSL: Segmentation, options: SegmentationQuery): String {
        return jinja.render(standard.trimIndent(), getMap(context, queryDSL)).trimIndent()
    }

    companion object {
        val standard = this::class.java.getResource("/sql/segmentation/warehouse/ansi/standard.jinja2").readText()
    }
}
