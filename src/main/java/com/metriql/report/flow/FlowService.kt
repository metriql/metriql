package com.metriql.report.flow

import com.metriql.report.IAdHocService
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.DatasetName
import com.metriql.util.ValidationUtil.stripLiteral
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.flow.Flow
import com.metriql.warehouse.spi.services.flow.FlowQueryGenerator
import javax.inject.Inject

class FlowService @Inject constructor(private val segmentationService: SegmentationService) : IAdHocService<FlowQuery> {

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: FlowQuery,
        reportFilters: ReportFilter?
    ): IAdHocService.RenderedQuery {
        reportOptions.event

        context.getModel(reportOptions.event.dataset)

        val requiredDimensions = listOf(
            // TODO: connector
            Recipe.FieldReference.fromName(":userId"),
            Recipe.FieldReference.fromName(":eventTimestamp")
        )

        val followingEvents = reportOptions.events.joinToString(" UNION ALL ") { event ->
            val dimension = event.dimension?.let { "CONCAT('${stripLiteral(event.dataset)}', CONCAT(' ', ${context.getDimensionAlias(it.name, it.relation, null)}))" }
                ?: "'${stripLiteral(event.dataset)}'"

            """SELECT user_id, event_timestamp, $dimension as event_type FROM (${
                segmentationService.renderQuery(
                    auth,
                    context,
                    SegmentationQuery(
                        event.dataset,
                        (requiredDimensions + (event.dimension?.let { listOf(it) } ?: listOf())),
                        listOf(),
                        event.filters
                    ),
                    reportFilters,
                    useAggregate = false, forAccumulator = false
                ).second
            }) t"""
        }

        val firstEvent = """SELECT user_id, event_timestamp, null as event_type FROM (${
            segmentationService.renderQuery(
                auth,
                context,
                SegmentationQuery(reportOptions.event.dataset, listOf(), requiredDimensions, reportOptions.event.filters),
                reportFilters,
                useAggregate = false, forAccumulator = false
            ).second
        })"""

        val flow = Flow(allEventsReference = """$firstEvent t UNION ALL $followingEvents""")

        val queryGenerator = context.datasource.warehouse.bridge.queryGenerators[FlowReportType.slug]
        val sqlQuery = (queryGenerator as? FlowQueryGenerator)?.generateSQL(auth, context, flow, reportOptions)
            ?: throw IllegalArgumentException("Warehouse query generator must be ${FlowQueryGenerator.javaClass.name} but it's ${queryGenerator?.javaClass?.name}")

        return IAdHocService.RenderedQuery(sqlQuery, listOf())
    }

    override fun getUsedDatasets(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: FlowQuery): Set<DatasetName> {
        return reportOptions.events.map { it.dataset }.toSet() + setOf(reportOptions.event.dataset)
    }
}
