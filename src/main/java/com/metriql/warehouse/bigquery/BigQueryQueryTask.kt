package com.metriql.warehouse.bigquery

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.JobStatistics
import com.google.cloud.bigquery.JobStatus.State.DONE
import com.google.cloud.bigquery.JobStatus.State.PENDING
import com.google.cloud.bigquery.JobStatus.State.RUNNING
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.TableResult
import com.metriql.audit.MetriqlEvents
import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.QueryStats
import com.metriql.report.QueryTask
import com.metriql.util.MetriqlEventBus
import com.metriql.util.MetriqlException
import com.metriql.util.`try?`
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.WarehouseAuth
import io.netty.handler.codec.http.HttpResponseStatus
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId

class BigQueryQueryTask(
    private val bigQuery: BigQuery,
    query: String,
    project: String,
    dataset: String,
    private val auth: WarehouseAuth,
    maximumBytesBilled: Long?,
    override val limit: Int,
    override val isBackgroundTask: Boolean
) : WarehouseQueryTask, QueryTask(auth.projectId, auth.userId, isBackgroundTask) {

    private val jobId: JobId
    private var queryConfig: QueryJobConfiguration
    private var queryJob: Job

    init {
        var queryConfigBuilder = QueryJobConfiguration
            .newBuilder(query)
            .setUseQueryCache(true)
            .setDefaultDataset(DatasetId.of(project, dataset))

        if (maximumBytesBilled != null && maximumBytesBilled > 0) {
            queryConfigBuilder = queryConfigBuilder.setMaximumBytesBilled(maximumBytesBilled)
        }
        queryConfig = queryConfigBuilder.build()

        try {
            queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).build())
        } catch (e: Exception) {
            throw MetriqlException("Unable to access BigQuery: ${e.message}", HttpResponseStatus.BAD_REQUEST)
        }
        jobId = queryJob.jobId
    }

    override fun run() {
        try {
            queryJob.waitFor()
            if (queryJob.status.error != null) {
                val errorMessage = queryJob.status.error?.message
                val exception = MetriqlException(errorMessage, HttpResponseStatus.BAD_REQUEST)
                MetriqlEventBus.publish(MetriqlEvents.InternalException(exception as Throwable, auth.userId, auth.projectId))
                setResult(QueryResult.errorResult(QueryResult.QueryError.create(errorMessage ?: "N/A")))
            }
            val tableResult = queryJob.getQueryResults(BigQuery.QueryResultsOption.pageSize(limit.toLong()))
            val queryResult = getQueryResultFromTableResult(tableResult.schema, tableResult)

            setResult(queryResult)
        } catch (e: Throwable) {
            setResult(QueryResult.errorResult(QueryResult.QueryError.create(e.message ?: "N/A")))
        }
    }

    override fun getStats(): QueryStats {
        val job = bigQuery.getJob(
            queryJob.jobId,
            BigQuery.JobOption.fields(
                BigQuery.JobField.STATISTICS,
                BigQuery.JobField.STATUS
            )
        )
        val jobStatistics = job.getStatistics<JobStatistics.QueryStatistics>()
        val timeline = jobStatistics.timeline
        if (timeline == null || timeline.size <= 0) {
            return QueryStats(QueryStats.State.CONNECTING_TO_DATABASE.description, nodes = 1, id = job.jobId.job)
        }
        val estimateBytesProcessed = job.getStatistics<JobStatistics.QueryStatistics>().estimatedBytesProcessed ?: 0
        val totalBytesProcessed = job.getStatistics<JobStatistics.QueryStatistics>().totalBytesProcessed ?: 0
        val percentage = (totalBytesProcessed + 1.0) / (estimateBytesProcessed + 1.0)

        val activeUnits = (timeline.last().activeUnits ?: 0).toInt()
        val elapsedTimeMillis = (timeline.last().elapsedMs ?: 0)
        val baseStats = QueryStats(
            QueryStats.State.CONNECTING_TO_DATABASE.description,
            id = job.jobId.job,
            processedBytes = totalBytesProcessed,
            elapsedTimeMillis = elapsedTimeMillis,
            percentage = percentage
        )
        return when (job.status.state) {
            PENDING -> baseStats.copy(state = QueryStats.State.CONNECTING_TO_DATABASE.description, id = job.jobId.job, nodes = activeUnits)
            RUNNING -> baseStats.copy(state = QueryStats.State.RUNNING.description, id = job.jobId.job, nodes = activeUnits)
            DONE -> baseStats.copy(state = QueryStats.State.FINISHED.description, percentage = 100.0, id = job.jobId.job, nodes = activeUnits)
            else -> QueryStats(QueryStats.State.CONNECTING_TO_DATABASE.description, id = job.jobId.job, nodes = activeUnits)
        }
    }

    override fun cancel() {
        super.cancel()
        if (this.queryJob != null) {
            `try?` { this.queryJob.cancel() }
        }
    }

    companion object {
        fun getQueryResultFromTableResult(tableSchema: Schema, tableResult: TableResult): QueryResult {
            var index = 0
            val meta = tableSchema
                .fields
                .map { field ->
                    if (field.type == LegacySQLTypeName.RECORD && field.subFields.isNotEmpty()) {
                        field.subFields
                            .map { subField ->
                                val col = QueryResult.QueryColumn(
                                    "${field.name}.${subField.name}",
                                    index,
                                    BigQueryDataSource.toFieldType(subField.type, subField.mode == Field.Mode.REPEATED)
                                )
                                index += 1
                                col
                            }
                    } else {
                        val col = listOf(
                            QueryResult.QueryColumn(
                                field.name,
                                index,
                                BigQueryDataSource.toFieldType(
                                    field.type,
                                    field.mode == Field.Mode.REPEATED
                                )
                            )
                        )
                        index += 1
                        col
                    }
                }.flatten()

            val result = tableResult
                .values
                // .take(hardLimit ?: 10000)
                .map { row ->
                    (0 until row.size)
                        .map { idx ->
                            rowValue(tableSchema.fields[idx], row[idx])
                        }.flatten()
                }
            return QueryResult(meta, result)
        }

        private fun rowValue(field: Field, row: FieldValue?): List<Any?> {
            if (row == null || row.isNull) return listOf(null)
            return try {
                when (field.mode) {
                    Field.Mode.REPEATED -> {
                        when (field.type) {
                            LegacySQLTypeName.NUMERIC, LegacySQLTypeName.INTEGER -> {
                                listOf(row.repeatedValue.joinToString(", ") { it.numericValue.toString() })
                            }
                            LegacySQLTypeName.FLOAT -> {
                                listOf(row.repeatedValue.joinToString(", ") { it.doubleValue.toString() })
                            }
                            LegacySQLTypeName.BOOLEAN -> {
                                listOf(row.repeatedValue.joinToString(", ") { it.booleanValue.toString() })
                            }
                            LegacySQLTypeName.TIME -> {
                                listOf(row.repeatedValue.joinToString(", ") { LocalTime.parse(it.stringValue).toString() })
                            }
                            LegacySQLTypeName.DATE -> {
                                listOf(row.repeatedValue.joinToString(", ") { LocalDate.parse(it.stringValue).toString() })
                            }
                            LegacySQLTypeName.TIMESTAMP -> {
                                // LocalDateTime does not have a timezone appended to iso format. Workaround for bigquery timestamp conversion issue
                                listOf(
                                    row.repeatedValue.joinToString(", ") {
                                        LocalDateTime.ofInstant(Instant.ofEpochMilli(it.timestampValue / 1000), ZoneId.of("UTC")).toString()
                                    }
                                )
                            }
                            LegacySQLTypeName.STRING -> {
                                listOf(row.repeatedValue.joinToString(", ") { it.stringValue.toString() })
                            }
                            LegacySQLTypeName.RECORD -> {
                                var index = 0
                                val recordList = mutableListOf<Any?>()
                                for (subField in field.subFields) {
                                    val recordValue = `try?` { row.recordValue[index] }
                                    recordList.add(rowValue(subField, recordValue))
                                    index += 1
                                }
                                recordList
                            }
                            else -> listOf(null) // row.value.toString()
                        }
                    }
                    null, Field.Mode.NULLABLE, Field.Mode.REQUIRED -> {
                        when (field.type) {
                            LegacySQLTypeName.NUMERIC, LegacySQLTypeName.INTEGER -> listOf(row.numericValue)
                            LegacySQLTypeName.FLOAT -> listOf(row.doubleValue)
                            LegacySQLTypeName.BOOLEAN -> listOf(row.booleanValue)
                            LegacySQLTypeName.TIME -> listOf(LocalTime.parse(row.stringValue))
                            LegacySQLTypeName.DATE -> listOf(LocalDate.parse(row.stringValue))
                            LegacySQLTypeName.TIMESTAMP -> listOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(row.timestampValue / 1000), ZoneId.of("UTC")))
                            LegacySQLTypeName.STRING -> listOf(row.stringValue)
                            LegacySQLTypeName.RECORD -> {
                                var index = 0
                                val recordList = mutableListOf<Any?>()
                                for (subField in field.subFields) {
                                    val recordValue = `try?` { row.recordValue[index] }
                                    recordList.add(rowValue(subField, recordValue))
                                    index += 1
                                }
                                recordList
                            }
                            else -> listOf(row.value.toString())
                        }
                    }
                }
            } catch (_: Throwable) {
                listOf(null)
            }
        }
    }
}
