package com.metriql.report.funnel

import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.PropertyKey.SUMMARIZED
import com.metriql.report.IAdHocService
import com.metriql.report.data.Dataset
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.IDatasetService
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.TIME_SERIES
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.USER_ID
import com.metriql.service.dataset.DatasetName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.funnel.Funnel
import com.metriql.warehouse.spi.services.funnel.Funnel.ExcludedStep
import com.metriql.warehouse.spi.services.funnel.Funnel.Step
import com.metriql.warehouse.spi.services.funnel.FunnelQueryGenerator
import io.netty.handler.codec.http.HttpResponseStatus
import javax.inject.Inject

class FunnelService @Inject constructor(
    private val datasetService: IDatasetService,
    private val segmentationService: SegmentationService,
) : IAdHocService<FunnelQuery> {

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        options: FunnelQuery,
        reportFilters: ReportFilter?,
    ): IAdHocService.RenderedQuery {
        val funnel = Funnel(
            steps = options.steps
                .mapIndexed { idx, step ->
                    renderStep(context, step, idx, auth, options, reportFilters, false)
                },
            excludedSteps = options.excludedSteps
                ?.mapIndexed { idx, exStep ->
                    ExcludedStep(
                        step = renderStep(
                            context, exStep.step, idx, auth,
                            options, reportFilters, true
                        ),
                        start = exStep.start
                    )
                },
            hasDimension = options.dimension != null,
            windowInSeconds = options.window?.toSeconds(),
            sorting = if (options.dimension?.reference?.getType(context, options.steps[0].dataset)?.second != FieldType.TIMESTAMP) "dimension" else "step1",
        )

        val queryGenerator = context.datasource.warehouse.bridge.queryGenerators[FunnelReportType.slug]
        val sqlQuery = (queryGenerator as? FunnelQueryGenerator)?.generateSQL(auth, context, funnel, options)
            ?: throw IllegalArgumentException("Warehouse query generator must be FunnelQueryGenerator")

        return IAdHocService.RenderedQuery(sqlQuery, listOf(::postProcess))
    }

    private fun renderStep(
        context: IQueryGeneratorContext,
        step: Dataset,
        idx: Int,
        auth: ProjectAuth,
        options: FunnelQuery,
        reportFilters: ReportFilter?,
        isExcludeStep: Boolean,
    ): Step {
        val mappings by lazy { datasetService.getDataset(auth, step.dataset)?.mappings }
        val connectorDimensionName = options.connector ?: (
            mappings?.get(USER_ID)
                ?: throw MetriqlException("`userId` mapping dimension is required for `${step.dataset}` model", HttpResponseStatus.BAD_REQUEST)
            )
        val eventTimeStampDimensionName = mappings?.get(TIME_SERIES)
            ?: throw MetriqlException("`eventTimestamp` mapping is required for `${step.dataset}` model", HttpResponseStatus.BAD_REQUEST)
        val contextModelName = step.dataset

        val dimensionsToRender = listOf(
            Recipe.FieldReference(connectorDimensionName),
            Recipe.FieldReference(eventTimeStampDimensionName)
        ) + (
            if (!isExcludeStep && options.dimension != null && options.dimension.step == idx) {
                val dimension = options.dimension.reference.toDimension(contextModelName, options.dimension.reference.getType(context, contextModelName).second)
                listOf(
                    ReportMetric.ReportDimension(
                        dimension.name,
                        contextModelName,
                        dimension.relation,
                        dimension.timeframe
                    ).toReference()
                )
            } else listOf()
            )

        return Step(
            index = idx + 1, // Base 1 indexing

            // To support join relations, we use segmentation query
            model = "(${
                segmentationService.renderQuery(
                    auth,
                    context,
                    SegmentationQuery(
                        contextModelName,
                        dimensionsToRender,
                        listOf(),
                        filters = step.filters
                    ),
                    reportFilters,
                    useAggregate = false,
                    forAccumulator = false
                ).second
            }) AS ${context.datasource.warehouse.bridge.quoteIdentifier(contextModelName)}",

            connector = context.datasource.warehouse.bridge.quoteIdentifier(connectorDimensionName),
            eventTimestamp = context.datasource.warehouse.bridge.quoteIdentifier(eventTimeStampDimensionName),

            dimension = if (!isExcludeStep && options.dimension != null && options.dimension.step == idx) {
                val dimension = options.dimension.reference.toDimension(contextModelName, options.dimension.reference.getType(context, contextModelName).second)

                // Pass context models as nulls while funnel does not support joins
                val alias = context.getDimensionAlias(
                    dimension.name,
                    dimension.relation,
                    dimension.timeframe
                )
                context.datasource.warehouse.bridge.quoteIdentifier(alias)
            } else {
                null
            },
            filters = null
        )
    }

    private fun postProcess(
        result: QueryResult,
    ): QueryResult {
        if (result.result != null) {
            result.setProperty(SUMMARIZED, false)
        }
        return result
    }

    override fun getUsedDatasets(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: FunnelQuery): Set<DatasetName> {
        return reportOptions.steps.flatMap { it.getUsedModels(context) }.toSet()
    }
}
