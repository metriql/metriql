package com.metriql.report.flow

import com.metriql.report.IAdHocService
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.ModelName
import com.metriql.util.ValidationUtil.stripLiteral
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceType
import com.metriql.warehouse.spi.services.flow.Flow
import com.metriql.warehouse.spi.services.flow.FlowQueryGenerator
import javax.inject.Inject

class FlowService @Inject constructor(private val segmentationService: SegmentationService) : IAdHocService<FlowReportOptions> {

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: FlowReportOptions,
        reportFilters: List<ReportFilter>,
    ): IAdHocService.RenderedQuery {
        reportOptions.event

        context.getModel(reportOptions.event.modelName)

        val requiredDimensions = listOf(
            // TODO: connector
            Recipe.FieldReference.fromName(":userId"),
            Recipe.FieldReference.fromName(":eventTimestamp")
        )

        val followingEvents = reportOptions.events.joinToString(" UNION ALL ") { event ->
            val dimension = event.dimension?.let { "CONCAT('${stripLiteral(event.modelName)}', CONCAT(' ', ${context.getDimensionAlias(it.name, it.relationName, null)}))" }
                ?: "'${stripLiteral(event.modelName)}'"

            """SELECT user_id, event_timestamp, $dimension as event_type FROM (${
            segmentationService.renderQuery(
                auth,
                context,
                SegmentationRecipeQuery(
                    event.modelName,
                    listOf(),
                    requiredDimensions + (event.dimension?.let { listOf(it.toReference()) } ?: listOf()),
                    event.filters.mapNotNull { it.toReference() }
                ).toReportOptions(context),
                reportFilters,
                useAggregate = false, forAccumulator = false
            ).second
            }) t"""
        }

        val firstEvent = """SELECT user_id, event_timestamp, null as event_type FROM (${
        segmentationService.renderQuery(
            auth,
            context,
            SegmentationRecipeQuery(reportOptions.event.modelName, listOf(), requiredDimensions, reportOptions.event.filters.mapNotNull { it.toReference() }).toReportOptions(
                context
            ),
            reportFilters,
            useAggregate = false, forAccumulator = false
        ).second
        })"""

        val flow = Flow(allEventsReference = """$firstEvent t UNION ALL $followingEvents""")

        val queryGenerator = context.datasource.warehouse.bridge.queryGenerators[ServiceType.FLOW]
        val sqlQuery = (queryGenerator as? FlowQueryGenerator)?.generateSQL(auth, context, flow, reportOptions)
            ?: throw IllegalArgumentException("Warehouse query generator must be ${FlowQueryGenerator.javaClass.name} but it's ${queryGenerator?.javaClass?.name}")

        return IAdHocService.RenderedQuery(sqlQuery, listOf())
    }

    override fun getUsedModels(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: FlowReportOptions): Set<ModelName> {
        return reportOptions.events.map { it.modelName }.toSet() + setOf(reportOptions.event.modelName)
    }
}
