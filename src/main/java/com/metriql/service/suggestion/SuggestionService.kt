package com.metriql.service.suggestion

import com.metriql.deployment.Deployment
import com.metriql.report.QueryTask
import com.metriql.report.ReportService
import com.metriql.report.SqlQueryTaskGenerator
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationQuery
import com.metriql.report.segmentation.SegmentationService
import com.metriql.report.sql.SqlQuery
import com.metriql.service.audit.MetriqlEvents.AuditLog.SQLExecuteEvent.SQLContext
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.DimensionName
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.TIME_SERIES
import com.metriql.service.dataset.DatasetName
import com.metriql.service.task.Task
import com.metriql.service.task.TaskQueueService
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import com.metriql.warehouse.spi.querycontext.TOTAL_ROWS_MEASURE
import io.netty.handler.codec.http.HttpResponseStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger
import javax.inject.Inject

class SuggestionService @Inject constructor(
    private val deployment: Deployment,
    private val segmentationService: SegmentationService,
    private val reportService: ReportService,
    private val sqlQueryTaskGenerator: SqlQueryTaskGenerator,
    private val taskQueueService: TaskQueueService,
) {
    fun search(
        auth: ProjectAuth,
        value: ReportMetric,
        filter: String?,
    ): CompletableFuture<List<String>> {
        val (modelName, dimensionName) = when (value) {
            is ReportMetric.ReportMappingDimension -> {
                deployment.getDatasetService().list(auth)
                    .firstNotNullOfOrNull { it.mappings.get(value.name)?.let { dim -> it.name to dim } }
                    ?: throw MetriqlException(HttpResponseStatus.NOT_FOUND)
            }

            is ReportMetric.ReportDimension -> {
                val sourceModelName = value.dataset!!
                val realModelName = if (value.relation != null) {
                    deployment.getDatasetService().getDataset(auth, sourceModelName)?.relations?.find { value.relation == it.name }?.datasetName
                        ?: throw MetriqlException(HttpResponseStatus.NOT_FOUND)
                } else {
                    sourceModelName
                }

                realModelName to value.name
            }
            // not supported
            else -> return CompletableFuture.completedFuture(listOf())
        }

        val suggestionsInCache = if (filter.isNullOrEmpty()) {
            deployment.getCacheService().getCommon(auth, modelName, dimensionName)
        } else {
            val filterExp = "%" + filter.replace("%".toRegex(), "\\%").replace("_".toRegex(), "\\_") + "%"
            deployment.getCacheService().search(auth, modelName, dimensionName, filterExp)
        }

        val task = if (suggestionsInCache != null) {
            val now = Instant.now()
            val isEmpty = suggestionsInCache.items.isEmpty()
            val isExpired = suggestionsInCache.lastUpdated.plus(30, ChronoUnit.DAYS).isBefore(now)
            if (isExpired || isEmpty) {
                fetchUniqueAttributeValues(auth, modelName, dimensionName, filter, useIncrementalDateFilter = !isEmpty)
            } else {
                return CompletableFuture.completedFuture(suggestionsInCache.items)
            }
        } else {
            fetchUniqueAttributeValues(auth, modelName, dimensionName, filter)
        }

        return taskQueueService.execute(
            task, 59
        ).thenApply { result ->
            val taskTicket = result.taskTicket()
            if (taskTicket.result?.error != null) {
                throw MetriqlException("Unable to fetch suggestions for dataset $modelName.$dimensionName: ${taskTicket.result?.error}", HttpResponseStatus.INTERNAL_SERVER_ERROR)
            } else
                if (result.status == Task.Status.FINISHED) {
                    val result = taskTicket.result?.result?.map { it[0].toString() } ?: listOf()
                    if (filter == null) {
                        result.take(50)
                    } else {
                        result.filter { it.contains(filter) }.take(50)
                    }
                } else {
                    listOf()
                }
        }
    }

    private fun fetchUniqueAttributeValues(
        auth: ProjectAuth,
        datasetName: DatasetName,
        dimensionName: DimensionName,
        filterText: String?,
        useIncrementalDateFilter: Boolean = true,
    ): QueryTask {
        val mapping = deployment.getDatasetService().getDataset(auth, datasetName)?.mappings
        val datasource = deployment.getDataSource(auth)
        val rakamBridge = datasource.warehouse.bridge
        val context = reportService.createContext(auth, datasource)

        // If mapping has incremental column it for filtering
        val canUseIncrementalFilter = mapping?.get(TIME_SERIES)?.isBlank() == false &&
            rakamBridge.filters.timestampOperators[TimestampOperatorType.GREATER_THAN] != null &&
            useIncrementalDateFilter

        val filters = if (canUseIncrementalFilter) {
            ReportFilter(
                MetricFilter(
                    MetricFilter.Connector.AND,
                    listOf(
                        MetricFilter.Filter(
                            Recipe.FieldReference.mappingDimension(TIME_SERIES, null),
                            TimestampOperatorType.BETWEEN.name,
                            "P2W"
                        )

                    )
                )
            )
        } else null

        val segmentationQuery = segmentationService.renderQuery(
            auth,
            context,
            SegmentationQuery(
                datasetName, listOf(Recipe.FieldReference(dimensionName)),
                listOf(Recipe.FieldReference(TOTAL_ROWS_MEASURE.name)), filters = filters
            ),
            reportFilters = null,
            useAggregate = false,
            forAccumulator = false
        )

        val executeTask = sqlQueryTaskGenerator.createTask(
            auth,
            context,
            datasource,
            segmentationQuery.second,
            SqlQuery.QueryOptions(100, null, null, true),
            true,
            info = SQLContext.Suggestion(datasetName, dimensionName, filterText)
        )

        executeTask.onFinish { result ->
            if (result?.error == null) {
                val values = result?.result?.map { it[0].toString() } ?: listOf()
                deployment.getCacheService().set(auth, datasetName, dimensionName, values)
            }
        }

        return executeTask
    }

    data class SuggestionQuery(
        val type: MetricFilter.MetricType,
        @PolymorphicTypeStr<MetricFilter.MetricType>(externalProperty = "type", valuesEnum = MetricFilter.MetricType::class)
        val value: ReportMetric,
        val filter: String?,
    )

    companion object {
        private val LOGGER = Logger.getLogger(this::class.java.name)
    }
}
