package com.metriql.db.bigquery

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.DatasetInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.StandardSQLTypeName
import com.metriql.db.TestingServer
import com.metriql.util.JsonHelper
import com.metriql.util.`try?`
import com.metriql.warehouse.bigquery.BigQueryWarehouse
import com.mockrunner.mock.jdbc.MockResultSet
import net.snowflake.client.jdbc.internal.amazonaws.util.StringInputStream
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime

object TestingEnvironmentBigQuery : TestingServer<Unit, BigQuery>() {

    override val config = JsonHelper.read(
        System.getenv("METRIQL_TEST_BIGQUERY_CREDENTIALS")
            ?: throw IllegalStateException("METRIQL_BIGQUERY_CREDENTIALS environment variable is required to run the tests"),
        BigQueryWarehouse.BigQueryConfig::class.java
    )!!

    private val bigQuery = BigQueryOptions
        .newBuilder()
        .setProjectId(config.project)
        .setCredentials(ServiceAccountCredentials.fromStream(StringInputStream(config.serviceAccountJSON)))
        .build()
        .service

    override fun getTableReference(tableName: String): String {
        return "`${config.dataset}`.`$tableName`"
    }

    override fun createConnection(): BigQuery {
        return bigQuery
    }

    @Synchronized
    override fun init() {
        // delete if exists
        `try?` { bigQuery.delete(DatasetId.of(config.project, config.dataset)) }
        bigQuery.create(DatasetInfo.newBuilder(config.project, config.dataset).build())
    }

    override fun resultSetFor(query: String): ResultSet {
        val bigQuery = createConnection()
        val rs = MockResultSet("resultSet")
        val queryResult = bigQuery.query(QueryJobConfiguration.of(query))
        queryResult.schema.fields.forEach { rs.addColumn(it.name) }
        val result = queryResult.iterateAll()
            .map { row ->
                (0 until queryResult.schema.fields.size)
                    .map { idx ->
                        when (queryResult.schema.fields[idx].type.standardType) {
                            StandardSQLTypeName.NUMERIC, StandardSQLTypeName.INT64 -> row[idx].numericValue
                            StandardSQLTypeName.FLOAT64 -> row[idx].doubleValue
                            StandardSQLTypeName.BOOL -> row[idx].booleanValue
                            StandardSQLTypeName.TIME -> LocalTime.parse(row[idx].stringValue)
                            StandardSQLTypeName.DATE -> LocalDate.parse(row[idx].stringValue)
                            StandardSQLTypeName.TIMESTAMP -> Timestamp.from(Instant.ofEpochMilli(row[idx].timestampValue / 1000))
                            else -> row[idx].value
                        }
                    }
            }
        result.forEach { rs.addRow(it) }
        return rs
    }
}
