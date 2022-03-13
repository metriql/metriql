package com.metriql.report.retention

import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.report.IAdHocService
import com.metriql.report.data.Dataset
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.getUsedModels
import com.metriql.report.retention.RetentionReportOptions.DateUnit.DAY
import com.metriql.report.retention.RetentionReportOptions.DateUnit.MONTH
import com.metriql.report.retention.RetentionReportOptions.DateUnit.WEEK
import com.metriql.report.segmentation.SegmentationReportOptions
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IDatasetService
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.USER_ID
import com.metriql.service.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.util.serializableName
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.retention.Retention
import com.metriql.warehouse.spi.services.retention.RetentionQueryGenerator
import io.netty.handler.codec.http.HttpResponseStatus
import java.time.LocalDate
import java.util.ArrayList
import java.util.Arrays
import javax.inject.Inject

class RetentionService @Inject constructor(
    private val modelService: IDatasetService,
    private val segmentationService: SegmentationService,
) : IAdHocService<RetentionReportOptions> {

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        report: RetentionReportOptions,
        reportFilters: List<ReportFilter>,
    ): IAdHocService.RenderedQuery {
        fun stepForRetentionStep(aStep: Dataset, isFirst: Boolean): Retention.Step {
            val mappings = modelService.getDataset(auth, aStep.modelName)?.mappings
            val connectorDimensionName = report.connector ?: (
                mappings?.get(USER_ID)
                    ?: throw MetriqlException("userId dimension is required for using connectors", HttpResponseStatus.BAD_REQUEST)
                )
            val eventTimeStampDimensionName = mappings?.get(EVENT_TIMESTAMP)
                ?: throw MetriqlException("userId dimension is required for using connectors", HttpResponseStatus.BAD_REQUEST)
            val contextModelName = aStep.modelName

            val eventTimestampPostOperation = when (report.dateUnit) {
                DAY -> ReportMetric.PostOperation(ReportMetric.PostOperation.Type.TIMESTAMP, TimestampPostOperation.DAY)
                WEEK -> ReportMetric.PostOperation(ReportMetric.PostOperation.Type.TIMESTAMP, TimestampPostOperation.WEEK)
                MONTH -> ReportMetric.PostOperation(ReportMetric.PostOperation.Type.TIMESTAMP, TimestampPostOperation.MONTH)
            }

            val eventTimestampReportDimension = ReportMetric.ReportDimension(eventTimeStampDimensionName, contextModelName, null, eventTimestampPostOperation)
            val connectorReportDimension = ReportMetric.ReportDimension(connectorDimensionName, contextModelName, null, null)
            val dimensionsToRender = if (report.dimension != null) {
                listOf(
                    ReportMetric.ReportDimension(report.dimension, contextModelName, null, null),
                    connectorReportDimension,
                    eventTimestampReportDimension
                )
            } else {
                listOf(connectorReportDimension, eventTimestampReportDimension)
            }

            // If the eventTimestamp is in the past, we need to include the next days for the returningStep
            // in order to calculate the retention for the following days
            val normalizedReportFilters = if (!isFirst) {
                val now = LocalDate.now()
                reportFilters.map { reportFilter ->
                    if (((reportFilter.value as? ReportFilter.FilterValue.MetricFilter)?.metricValue as? ReportMetric.ReportMappingDimension)?.name == EVENT_TIMESTAMP) {
                        reportFilter.copy(
                            value = reportFilter.value.copy(
                                filters = reportFilter.value.filters.map {
                                    if (it.operator.lowercase() == "between" && it.value is Map<*, *>) {
                                        val endDate = LocalDate.parse(it.value["end"].toString())
                                        val maximumEnd = when (report.dateUnit) {
                                            DAY -> endDate.plusDays(14)
                                            WEEK -> endDate.plusWeeks(14)
                                            MONTH -> endDate.plusMonths(14)
                                        }
                                        val finalEnd = if (now > maximumEnd) maximumEnd else now
                                        it.copy(value = mapOf("start" to it.value["start"], "end" to finalEnd.toString()))
                                    } else {
                                        it
                                    }
                                }
                            )
                        )
                    } else {
                        reportFilter
                    }
                }
            } else {
                reportFilters
            }

            return Retention.Step(
                model = "(${
                segmentationService.renderQuery(
                    auth,
                    context,
                    SegmentationReportOptions(contextModelName, dimensionsToRender, listOf(), filters = aStep.filters),
                    normalizedReportFilters,
                    useAggregate = false, forAccumulator = false
                ).second
                }) AS $contextModelName",
                // While the modelReference is rendered by the segmentation service, these dimension values must always refer to dimension aliases.
                connector = context.warehouseBridge.quoteIdentifier(context.getDimensionAlias(connectorDimensionName, null, null)),
                // Also pass the post-operator to alias generator, since post operated dimensions has prefix of the post operation name
                // i.e timestamp day post operated _time dimensions alias is: column as _time_timestamp_day
                eventTimestamp = context.warehouseBridge.quoteIdentifier(
                    context.getDimensionAlias(
                        eventTimeStampDimensionName,
                        null,
                        eventTimestampPostOperation
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

    override fun getUsedModels(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: RetentionReportOptions): Set<ModelName> {
        return (getUsedModels(reportOptions.firstStep, context) + getUsedModels(reportOptions.returningStep, context)).toSet()
    }
}
