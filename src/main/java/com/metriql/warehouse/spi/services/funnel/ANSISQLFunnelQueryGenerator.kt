package com.metriql.warehouse.spi.services.funnel

import com.metriql.report.funnel.FunnelQuery
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class ANSISQLFunnelQueryGenerator(private val template: String? = null, private val partitionSuffix: String? = null) : FunnelQueryGenerator {
    override fun generateSQL(auth: ProjectAuth, context: IQueryGeneratorContext, queryDSL: Funnel, options: FunnelQuery): String {

        checkSupport(options.strictlyOrdered, options)
        return jinja.render(
            template ?: genericTemplate,
            mapOf(
                "funnel" to queryDSL,
                "ordered" to options.strictlyOrdered,
                "has_timezone" to (auth.timezone != null),
            /*
            * If query includes view models (WITH view as ().., ne should discard the WITH prefix while rendering the query),
            * since view models are appended to query as WITH view as (), we have to only add ","
            * otherwise start with WITH keyword
            * {% if has_view_models %}, {% else %}WITH {% endif %} first_action AS (
            * */
                "has_view_models" to context.viewModels.isNotEmpty(),
                "partition_suffix" to (partitionSuffix ?: "")
            )
        )
    }

    override fun supports(): List<FunnelSupport> {
        return listOf(
            FunnelSupport(
                isStrictlyOrdered = true,
                approximation = false,
                dimension = true,
                window = true,
                exclusion = true
            ),
            FunnelSupport(
                isStrictlyOrdered = false,
                approximation = false,
                dimension = true,
                window = true,
                exclusion = true
            )
        )
    }

    companion object {
        private val genericTemplate = this::class.java.getResource("/sql/funnel/warehouse/ansi/generic.jinja2").readText()
    }
}
