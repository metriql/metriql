package com.metriql.report.funnel

import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.PropertyKey.SUMMARIZED
import com.metriql.report.IAdHocService
import com.metriql.report.data.Dataset
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.getUsedModels
import com.metriql.report.segmentation.SegmentationReportOptions
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IDatasetService
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.USER_ID
import com.metriql.service.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.funnel.Funnel
import com.metriql.warehouse.spi.services.funnel.Funnel.ExcludedStep
import com.metriql.warehouse.spi.services.funnel.Funnel.Step
import com.metriql.warehouse.spi.services.funnel.FunnelQueryGenerator
import io.netty.handler.codec.http.HttpResponseStatus
import javax.inject.Inject

class FunnelService @Inject constructor(
    private val modelService: IDatasetService,
    private val segmentationService: SegmentationService,
) : IAdHocService<FunnelReportOptions> {

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        options: FunnelReportOptions,
        reportFilters: List<ReportFilter>,
    ): IAdHocService.RenderedQuery {
        val funnel = Funnel(
            steps = options.steps
                .mapIndexed { idx, step ->
                    renderStep(context, step, idx, auth, context.datasource, options, reportFilters, false)
                },
            excludedSteps = options.excludedSteps
                ?.mapIndexed { idx, exStep ->
                    ExcludedStep(
                        step = renderStep(
                            context, exStep.step, idx, auth, context.datasource,
                            options, reportFilters, true
                        ),
                        start = exStep.start
                    )
                },
            hasDimension = options.dimension != null,
            windowInSeconds = options.window?.toSeconds(),
            sorting = if (options.dimension?.postOperation != null) "dimension" else "step1",
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
        datasource: DataSource,
        options: FunnelReportOptions,
        reportFilters: List<ReportFilter> = listOf(),
        isExcludeStep: Boolean,
    ): Step {
        val mappings by lazy { modelService.getDataset(auth, step.modelName)?.mappings }
        val connectorDimensionName = options.connector ?: (
            mappings?.get(USER_ID)
                ?: throw MetriqlException("`userId` mapping dimension is required for `${step.modelName}` model", HttpResponseStatus.BAD_REQUEST)
            )
        val eventTimeStampDimensionName = mappings?.get(EVENT_TIMESTAMP)
            ?: throw MetriqlException("`eventTimestamp` mapping is required for `${step.modelName}` model", HttpResponseStatus.BAD_REQUEST)
        val contextModelName = step.modelName

        val dimensionsToRender = listOf(
            ReportMetric.ReportDimension(connectorDimensionName, contextModelName, null, null),
            ReportMetric.ReportDimension(eventTimeStampDimensionName, contextModelName, null, null)
        ) + (
            if (!isExcludeStep && options.dimension != null && options.dimension.step == idx) {
                listOf(
                    ReportMetric.ReportDimension(
                        options.dimension.name,
                        contextModelName,
                        options.dimension.relationName,
                        options.dimension.postOperation
                    )
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
                SegmentationReportOptions(
                    contextModelName,
                    dimensionsToRender,
                    listOf(),
                    filters = step.filters,
                    reportOptions = null
                ),
                reportFilters,
                useAggregate = false,
                forAccumulator = false
            ).second
            }) AS ${datasource.warehouse.bridge.quoteIdentifier(contextModelName)}",

            connector = datasource.warehouse.bridge.quoteIdentifier(connectorDimensionName),
            eventTimestamp = datasource.warehouse.bridge.quoteIdentifier(eventTimeStampDimensionName),

            dimension = if (!isExcludeStep && options.dimension != null && options.dimension.step == idx) {
                // Pass context models as nulls while funnel does not support joins
                val alias = context.getDimensionAlias(
                    options.dimension.name,
                    options.dimension.relationName,
                    options.dimension.postOperation
                )
                datasource.warehouse.bridge.quoteIdentifier(alias)
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

    override fun getUsedModels(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: FunnelReportOptions): Set<ModelName> {
        return reportOptions.steps.flatMap { step -> getUsedModels(step, context) }.toSet()
    }
}
