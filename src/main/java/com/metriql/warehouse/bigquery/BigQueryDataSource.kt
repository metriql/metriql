package com.metriql.warehouse.bigquery

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQuery.TableField
import com.google.cloud.bigquery.BigQuery.TableOption
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableResult
import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.QueryStats.State.FINISHED
import com.metriql.report.QueryTask
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.Model
import com.metriql.service.task.Task
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.MetriqlExceptions
import com.metriql.util.ValidationUtil.quoteIdentifier
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.bigquery.BigQueryWarehouse.bridge
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.DatabaseName
import com.metriql.warehouse.spi.DbtSettings
import com.metriql.warehouse.spi.SchemaName
import com.metriql.warehouse.spi.TableName
import com.metriql.warehouse.spi.TableSchema
import com.metriql.warehouse.spi.WarehouseAuth
import io.netty.handler.codec.http.HttpResponseStatus
import net.snowflake.client.jdbc.internal.amazonaws.util.StringInputStream
import java.util.UUID

class BigQueryDataSource(override val config: BigQueryWarehouse.BigQueryConfig) : DataSource {
    override val warehouse = BigQueryWarehouse

    /** A public dataset can be the default project. However, we need to execute jobs on our own project */
    private val serviceProjectId: String by lazy {
        try {
            JsonHelper.read<ObjectNode>(config.serviceAccountJSON)["project_id"].asText()
        } catch (e: Exception) {
            throw MetriqlException("Service account is not valid.", HttpResponseStatus.BAD_REQUEST)
        }
    }

    private val bigQuery = BigQueryOptions
        .newBuilder()
        .setProjectId(serviceProjectId)
//        .setCredentials(OAuth2Credentials.newBuilder().setAccessToken("").build())
        .setCredentials(ServiceAccountCredentials.fromStream(StringInputStream(config.serviceAccountJSON)))
        .build()
        .service

    override fun preview(auth: WarehouseAuth, target: Model.Target): Task<*, *> {
        if (target.value is Model.Target.TargetValue.Table) {
            return object : QueryTask(auth.projectId, auth.userId, auth.source, false) {
                override fun run() {
                    val tableId = TableId.of(target.value.database ?: config.project ?: serviceProjectId, target.value.schema ?: config.dataset, target.value.table)
                    val bTable = bigQuery.getTable(tableId).reload(TableOption.fields(TableField.SCHEMA))
                    val tableSchema = bTable.getDefinition<TableDefinition>().schema!!
                    val tableResult = bTable.list(BigQuery.TableDataListOption.pageSize(100))
                    val queryResult = BigQueryQueryTask.getQueryResultFromTableResult(tableSchema, tableResult)
                    setResult(queryResult)
                }

                override fun getStats(): QueryResult.QueryStats {
                    return QueryResult.QueryStats(FINISHED, "preview($target)", nodes = 1, percentage = 100.0)
                }
            }
        } else {
            throw UnsupportedOperationException()
        }
    }

    private fun createSyncQueryJob(queryConfig: QueryJobConfiguration): TableResult {
        val jobId = JobId.of(UUID.randomUUID().toString())
        val queryJob = bigQuery
            .create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
            .waitFor()

        if (queryJob == null) {
            throw MetriqlException("Task not longer exists", HttpResponseStatus.BAD_REQUEST)
        } else if (queryJob.status.error != null) {
            throw MetriqlException(queryJob.status.error.toString(), HttpResponseStatus.BAD_REQUEST)
        }
        return queryJob.getQueryResults()
    }

    override fun connectionTest(userId: Int): Boolean {
        return try {
            try {
                createSyncQueryJob(QueryJobConfiguration.of("SELECT 1")).totalRows >= 1
            } catch (e: BigQueryException) {
                throw MetriqlException("Could not run query (bigquery.jobs.create): ${e.message}", HttpResponseStatus.FORBIDDEN)
            }

            try {
                bigQuery.getDataset(config.dataset) ?: throw MetriqlException(
                    "Dataset '${config.dataset}' does not exists in project '${config.project}'",
                    HttpResponseStatus.BAD_REQUEST
                )
            } catch (e: BigQueryException) {
                throw MetriqlException("Could not fetch the dataset: ${e.message}", HttpResponseStatus.FORBIDDEN)
            }

            try {
                bigQuery.listTables(config.dataset, BigQuery.TableListOption.pageSize(1))
            } catch (e: BigQueryException) {
                throw MetriqlException("Could not list tables (bigquery.tables.list): ${e.message}", HttpResponseStatus.FORBIDDEN)
            }

            try {
                bigQuery.getTable(config.dataset, "randomtable", TableOption.fields())
            } catch (e: Exception) {
                throw MetriqlException("Could not get table metadata (bigquery.tables.get): ${e.message}", HttpResponseStatus.FORBIDDEN)
            }

            true
        } catch (e: Exception) {
            throw MetriqlExceptions.SYSTEM_EXCEPTION_FROM_CAUSE.exceptionFromObject(e)
        }
    }

    override fun getTable(database: String?, schema: String?, table: String): TableSchema {
        val tableId = TableId.of(database ?: config.project, schema ?: config.dataset, table)
        val bigQueryTable = bigQuery.getTable(tableId) ?: throw MetriqlException("Table '$table' not found in $tableId", HttpResponseStatus.NOT_FOUND)
        val columns = bigQueryTable
            .reload(TableOption.fields(TableField.SCHEMA, TableField.DESCRIPTION, TableField.FRIENDLY_NAME))
            .getDefinition<TableDefinition>().schema!!
            .fields
            .map { column ->
                TableSchema.Column(
                    column.name, null, column.type.name(),
                    toFieldType(
                        column.type
                    ),
                    column.description
                )
            }
        return TableSchema(table, null, columns)
    }

    override fun getTable(sql: String): TableSchema {
        val query = "$sql LIMIT 0"
        val jobId = JobId.of(UUID.randomUUID().toString())
        val queryConfig = QueryJobConfiguration
            .newBuilder(query)
            .setUseQueryCache(true)
            .setDefaultDataset(DatasetId.of(config.project, config.dataset))
            .build()
        val queryJob = bigQuery
            .create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
        queryJob.waitFor()
        if (queryJob.status.error != null) {
            val errorMessage = queryJob.status.error?.message
            throw MetriqlException(errorMessage, HttpResponseStatus.BAD_REQUEST)
        }
        val columns = queryJob
            .getQueryResults()
            .schema
            .fields
            .map { column ->
                TableSchema.Column(
                    column.name, null, column.type.name(),
                    toFieldType(
                        column.type
                    ),
                    column.description
                )
            }
        return TableSchema("derived", null, columns)
    }

    override fun listTableNames(database: String?, schema: String?): List<TableName> {
        val datasetId = DatasetId.of(database ?: config.project ?: serviceProjectId, schema ?: config.dataset)
        // we fetch dataset first since the project might be different than the default configuration
        val dataset = bigQuery.getDataset(datasetId) ?: return listOf()

        return dataset
            .list(BigQuery.TableListOption.pageSize(250))
            .values
//            .filter { it.getDefinition<TableDefinition>().type == TableDefinition.Type.TABLE }
            .map { it.tableId.table }
    }

    override fun listSchema(
        database: String?,
        schema: String?,
        tables: Collection<String>?
    ): List<TableSchema> {
        val datasetId = DatasetId.of(database ?: config.project ?: serviceProjectId, schema ?: config.dataset)

        val bqTables = bigQuery
            .listTables(datasetId)
            .iterateAll()
            .filter { tables?.contains(it.tableId.table) ?: true } // Filter only requested tables before reloading schema
            .map { it.reload(TableOption.fields(TableField.SCHEMA, TableField.DESCRIPTION, TableField.FRIENDLY_NAME)) }
        return bqTables.map { table ->
            val tableName = table.tableId.table
            val columns = table.getDefinition<TableDefinition>().schema!!.fields.map { column ->
                if (column.type == LegacySQLTypeName.RECORD) {
                    column.subFields.map { subField ->
                        TableSchema.Column(
                            "${column.name}_${subField.name}",
                            "{{TABLE}}.`${column.name}`.`${subField.name}`",
                            subField.type.name(),
                            toFieldType(subField.type),
                            subField.description
                        )
                    }
                } else {
                    listOf(
                        TableSchema.Column(
                            column.name, null, column.type.name(),
                            toFieldType(
                                column.type
                            ),
                            column.description
                        )
                    )
                }
            }
            TableSchema(tableName, table.description, columns.flatten())
        }
    }

    override fun listDatabaseNames(): List<DatabaseName> {
        return listOf(config.project ?: serviceProjectId)
    }

    override fun listSchemaNames(database: String?): List<SchemaName> {
        return bigQuery.listDatasets(database ?: config.project ?: serviceProjectId)
            .iterateAll()
            .map { it.datasetId.dataset }
    }

    override fun createQueryTask(
        warehouseAuth: WarehouseAuth,
        query: String,
        defaultSchema: String?,
        defaultDatabase: String?,
        limit: Int?,
        isBackgroundTask: Boolean
    ): QueryTask {
        return BigQueryQueryTask(
            bigQuery,
            query,
            defaultDatabase ?: config.project ?: serviceProjectId,
            defaultSchema ?: config.dataset,
            warehouseAuth,
            config.maximumBytesBilled,
            limit ?: WarehouseQueryTask.DEFAULT_LIMIT,
            isBackgroundTask
        )
    }

    override fun dbtSettings(): DbtSettings {
        return DbtSettings(
            "bigquery",
            mapOf(
                "method" to "service-account-json",
                "keyfile_json" to JsonHelper.read(config.serviceAccountJSON, ObjectNode::class.java),
                "database" to (config.project ?: serviceProjectId),
                "schema" to (config.dataset)
            )
        )
    }

    override fun sqlReferenceForTarget(
        target: Model.Target,
        aliasName: String,
        columnName: String
    ) = "${bridge.quoteIdentifier(aliasName)}.${bridge.quoteIdentifier(columnName)}"

    override fun sqlReferenceForTarget(
        target: Model.Target,
        aliasName: String,
        renderSQL: (SQLRenderable) -> String
    ): String {
        return when (target.value) {
            is Model.Target.TargetValue.Sql -> renderSQL.invoke(target.value.sql)
            is Model.Target.TargetValue.Table -> {
                "${bridge.quoteIdentifier(target.value.database ?: config.project ?: serviceProjectId)}." +
                    "${bridge.quoteIdentifier(target.value.schema ?: config.dataset)}." +
                    "${bridge.quoteIdentifier(target.value.table)} AS ${bridge.quoteIdentifier(aliasName)}"
            }
        }
    }

    override fun fillDefaultsToTarget(target: Model.Target): Model.Target {
        return when (target.value) {
            is Model.Target.TargetValue.Sql -> target
            is Model.Target.TargetValue.Table -> {
                target.copy(value = target.value.copy(database = target.value.database ?: config.project, schema = target.value.schema ?: config.dataset))
            }
        }
    }

    companion object {
        fun toFieldType(rawType: LegacySQLTypeName, isArray: Boolean = false): FieldType? {
            if (isArray) {
                return when (rawType) {
                    LegacySQLTypeName.INTEGER -> FieldType.ARRAY_INTEGER
                    LegacySQLTypeName.BOOLEAN -> FieldType.ARRAY_BOOLEAN
                    LegacySQLTypeName.FLOAT -> FieldType.ARRAY_DOUBLE
                    LegacySQLTypeName.NUMERIC -> FieldType.ARRAY_DOUBLE
                    LegacySQLTypeName.STRING -> FieldType.ARRAY_STRING
                    LegacySQLTypeName.TIMESTAMP -> FieldType.ARRAY_TIMESTAMP
                    LegacySQLTypeName.DATE -> FieldType.ARRAY_DATE
                    LegacySQLTypeName.TIME -> FieldType.ARRAY_TIME
                    LegacySQLTypeName.DATETIME -> FieldType.ARRAY_TIMESTAMP
                    LegacySQLTypeName.RECORD, LegacySQLTypeName.GEOGRAPHY, LegacySQLTypeName.BYTES -> FieldType.UNKNOWN // Record should appear down
                    else -> FieldType.UNKNOWN
                }
            } else {
                return when (rawType) {
                    LegacySQLTypeName.INTEGER -> FieldType.INTEGER
                    LegacySQLTypeName.BOOLEAN -> FieldType.BOOLEAN
                    LegacySQLTypeName.FLOAT -> FieldType.DOUBLE
                    LegacySQLTypeName.NUMERIC -> FieldType.DOUBLE
                    LegacySQLTypeName.STRING -> FieldType.STRING
                    LegacySQLTypeName.RECORD -> FieldType.ARRAY_STRING
                    LegacySQLTypeName.TIMESTAMP -> FieldType.TIMESTAMP
                    LegacySQLTypeName.DATE -> FieldType.DATE
                    LegacySQLTypeName.TIME -> FieldType.TIME
                    LegacySQLTypeName.DATETIME -> FieldType.TIMESTAMP
                    LegacySQLTypeName.GEOGRAPHY, LegacySQLTypeName.BYTES -> FieldType.UNKNOWN
                    else -> FieldType.UNKNOWN
                }
            }
        }
    }
}
