package com.metriql.report.retention

import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.report.IAdHocService
import com.metriql.report.data.Dataset
import com.metriql.report.data.FilterValue
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.retention.RetentionQuery.DateUnit.DAY
import com.metriql.report.retention.RetentionQuery.DateUnit.MONTH
import com.metriql.report.retention.RetentionQuery.DateUnit.WEEK
import com.metriql.report.segmentation.SegmentationQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.IDatasetService
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.TIME_SERIES
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.USER_ID
import com.metriql.service.dataset.DatasetName
import com.metriql.util.MetriqlException
import com.metriql.util.serializableName
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.retention.Retention
import com.metriql.warehouse.spi.services.retention.RetentionQueryGenerator
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.ArrayList
import java.util.Arrays
import javax.inject.Inject

class RetentionService @Inject constructor(
    private val datasetService: IDatasetService,
    private val segmentationService: SegmentationService,
) : IAdHocService<RetentionQuery> {

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        report: RetentionQuery,
        reportFilters: FilterValue?,
    ): IAdHocService.RenderedQuery {
        fun stepForRetentionStep(aStep: Dataset, isFirst: Boolean): Retention.Step {
            val mappings = datasetService.getDataset(auth, aStep.dataset)?.mappings
            val connectorDimensionName = report.connector ?: (
                mappings?.get(USER_ID)
                    ?: throw MetriqlException("userId dimension is required for using connectors", HttpResponseStatus.BAD_REQUEST)
                )
            val eventTimeStampDimensionName = mappings?.get(TIME_SERIES)
                ?: throw MetriqlException("userId dimension is required for using connectors", HttpResponseStatus.BAD_REQUEST)
            val contextModelName = aStep.dataset

            val eventTimestampTimeframe = when (report.dateUnit) {
                DAY -> ReportMetric.Timeframe(ReportMetric.Timeframe.Type.TIMESTAMP, TimestampPostOperation.DAY)
                WEEK -> ReportMetric.Timeframe(ReportMetric.Timeframe.Type.TIMESTAMP, TimestampPostOperation.WEEK)
                MONTH -> ReportMetric.Timeframe(ReportMetric.Timeframe.Type.TIMESTAMP, TimestampPostOperation.MONTH)
            }

            val eventTimestampReportDimension = ReportMetric.ReportDimension(eventTimeStampDimensionName, contextModelName, null, eventTimestampTimeframe).toReference()
            val connectorReportDimension = Recipe.FieldReference(connectorDimensionName)
            val dimensionsToRender = if (report.dimension != null) {
                listOf(
                    Recipe.FieldReference(report.dimension),
                    connectorReportDimension,
                    eventTimestampReportDimension
                )
            } else {
                listOf(connectorReportDimension, eventTimestampReportDimension)
            }

            return Retention.Step(
                model = "(${
                    segmentationService.renderQuery(
                        auth,
                        context,
                        SegmentationQuery(contextModelName, dimensionsToRender, listOf(), filters = aStep.filters),
                        reportFilters,
                        useAggregate = false, forAccumulator = false
                    ).second
                }) AS $contextModelName",
                // While the modelReference is rendered by the segmentation service, these dimension values must always refer to dimension aliases.
                connector = context.warehouseBridge.quoteIdentifier(context.getDimensionAlias(connectorDimensionName, null, null)),
                // Also pass the post-operator to alias generator, since post operated dimensions has prefix of the timeframe name
                // i.e timestamp day post operated _time dimensions alias is: column as _time_timestamp_day
                eventTimestamp = context.warehouseBridge.quoteIdentifier(
                    context.getDimensionAlias(
                        eventTimeStampDimensionName,
                        null,
                        eventTimestampTimeframe
                    )
                ),
                dimension = if (report.dimension != null) {
                    // Pass context models as nulls while funnel does not support joins
                    context.warehouseBridge.quoteIdentifier(report.dimension)
                } else {
                    null
                },
                filters = null
            )
        }

        val retention = Retention(
            firstStep = stepForRetentionStep(report.firstStep, true),
            returningStep = stepForRetentionStep(report.returningStep, false),
            dateUnit = report.dateUnit.serializableName
        )

        val queryGenerator = context.datasource.warehouse.bridge.queryGenerators[RetentionReportType.slug]
        val sqlQuery = if (queryGenerator is RetentionQueryGenerator) {
            queryGenerator.generateSQL(auth, context, retention, report)
        } else {
            throw MetriqlException("Warehouse does not support retention query generation", HttpResponseStatus.BAD_REQUEST)
        }

        return IAdHocService.RenderedQuery(sqlQuery, listOf(::postProcess))
    }

    private fun postProcess(result: QueryResult): QueryResult {
        return if (result.error == null) {
            val rows: List<List<Any?>> = when (result.metadata!!.size) {
                2 -> {
                    val list = ArrayList<List<Any>>()

                    for (objects in result.result!!) {
                        val date = objects[0]
                        val days = objects[1] as Array<*>
                        for (i in days.indices) {
                            val day = if (i == 0) null else i.toLong() - 1
                            when {
                                days[i] == null -> list.add(Arrays.asList<Any>(date, day, null))
                                days[i] is Number -> {
                                    val value = (days[i] as Number).toLong()
                                    list.add(Arrays.asList<Any>(date, day, value))
                                }

                                else -> throw MetriqlException(
                                    "Query result is not valid",
                                    HttpResponseStatus.BAD_REQUEST
                                )
                            }
                        }
                    }

                    list
                }

                3 -> result.result!!
                else -> throw MetriqlException("Invalid result returned by retention query", HttpResponseStatus.INTERNAL_SERVER_ERROR)
            }

            QueryResult(
                listOf(
                    QueryResult.QueryColumn("dimension", 0, result.metadata[0].type),
                    QueryResult.QueryColumn("lead", 1, FieldType.INTEGER),
                    QueryResult.QueryColumn("value", 2, FieldType.INTEGER)
                ),
                rows, result.properties
            )
        } else {
            result
        }
    }

    override fun getUsedDatasets(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: RetentionQuery): Set<DatasetName> {
        return (reportOptions.firstStep.getUsedModels(context) + reportOptions.returningStep.getUsedModels(context)).toSet()
    }
}
