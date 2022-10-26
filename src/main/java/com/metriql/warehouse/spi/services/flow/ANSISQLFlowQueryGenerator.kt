package com.metriql.warehouse.spi.services.flow

import com.metriql.report.flow.FlowQuery
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class ANSISQLFlowQueryGenerator : FlowQueryGenerator {
    override fun generateSQL(auth: ProjectAuth, context: IQueryGeneratorContext, queryDSL: Flow, options: FlowQuery): String {
        return jinja.render(
            genericTemplate,
            mapOf(
                "dsl" to queryDSL,
                "query" to options,
                "has_view_models" to context.viewModels.isNotEmpty()
            )
        )
    }

    companion object {
        private var genericTemplate = """
            {% set window_func = 'lead' if query.isStartingEvent else 'lag' -%}
            {% if has_view_models %}, {% else %} with {% endif %} events as ({{ dsl.allEventsReference }})
            select {% for event in range(query.stepCount) %}event{{ loop.index }}, {% endfor %} count(distinct user_id) from (
            select user_id,
            {% for event in range(query.stepCount) %}
            CASE WHEN event_type IS NULL {% if query.window %}AND DATEDIFF('{{query.window.type}}', CAST({{window_func}}(event_timestamp, 1) over (partition by user_id order by event_timestamp) AS TIMESTAMP), CAST(event_timestamp AS TIMESTAMP)) < {{query.window.value}}{% endif %} then {{window_func}}(event_type, {{ loop.index }}) over (partition by user_id order by event_timestamp) end event{{ loop.index }}{% if not loop.last %},{% endif %}
            {% endfor %}
            from events
            ) t
            WHERE event1 IS NOT NULL
            GROUP BY {% for event in range(query.stepCount) %}{{ loop.index }}{% if not loop.last %},{% endif %} {% endfor %}
        """.trimIndent()
    }
}
