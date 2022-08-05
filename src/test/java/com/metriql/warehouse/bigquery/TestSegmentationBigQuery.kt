package com.metriql.warehouse.bigquery

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import com.metriql.tests.TestSegmentation
import com.metriql.util.`try?`
import org.testng.annotations.BeforeSuite
import java.sql.Date
import java.time.Instant
import java.util.UUID

class TestSegmentationBigQuery : TestSegmentation() {
    val testingServer = TestingEnvironmentBigQuery

    // Services
    override var dataSource = BigQueryDataSource(testingServer.config)

    @BeforeSuite
    @Throws(Exception::class)
    fun setup() {
        testingServer.init()
        fillData()
    }

    private fun fillData() {
        val bigQuery = testingServer.getQueryRunner()
        val mapField = Field
            .newBuilder("testmap", LegacySQLTypeName.RECORD, FieldList.of(Field.of("test", LegacySQLTypeName.STRING)))
            .setMode(Field.Mode.NULLABLE)
            .build()

        val arrayField = Field
            .newBuilder("testarray", StandardSQLTypeName.INT64)
            .setMode(Field.Mode.REPEATED)
            .build()

        listOf("_table", "_table2").forEach { tableName ->
            val fields = mutableListOf(
                Field.of("teststr", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.STRING)),
                Field.of("testnumber", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.FLOAT64)),
                Field.of("testbool", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.BOOL)),
                mapField,
                arrayField,
                Field.of("testdate", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.DATE)),
                Field.of("_time", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.TIMESTAMP))
            )

            if (tableName == "_table2") {
                fields.add(Field.of("testdummy", LegacySQLTypeName.legacySQLTypeName(StandardSQLTypeName.STRING)))
            }

            val schema = Schema.of(fields)
            val tableDefinition = StandardTableDefinition.of(schema)
            val tableId = TableId.of(testingServer.config.dataset, tableName)
            val tableInfo = TableInfo.of(tableId, tableDefinition)
            `try?` { bigQuery.getTable(tableId).delete() }
            bigQuery.create(tableInfo)

            val rows = (0 until SCALE_FACTOR).map { i ->
                val rows = mutableMapOf(
                    "teststr" to "test$i",
                    "testnumber" to i.toDouble(),
                    "testbool" to (i % 2 == 0),
                    "testmap" to mapOf("test" to i.toDouble()),
                    "testarray" to listOf(i.toDouble()),
                    "testdate" to "${Date(i.toLong())}",
                    "_time" to "${Instant.ofEpochMilli(i.toLong() * 1000 * 60 * 60 * 24)}"
                )
                if (tableName == "_table2") {
                    rows["testdummy"] = "test$i"
                }
                InsertAllRequest.RowToInsert.of(UUID.randomUUID().toString(), rows)
            }
            // https://github.com/googleapis/google-cloud-java/issues/3344
            bigQuery.getTable(tableId).insert(rows)
            bigQuery.getTable(tableId).insert(rows)
            bigQuery.getTable(tableId).insert(rows)
            bigQuery.getTable(tableId).insert(rows)
        }
    }
}
