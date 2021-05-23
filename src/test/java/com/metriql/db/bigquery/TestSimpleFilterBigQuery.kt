package com.metriql.db.bigquery

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import com.metriql.tests.SimpleFilterTests
import com.metriql.tests.TestSimpleFilter
import com.metriql.util.`try?`
import com.metriql.warehouse.bigquery.BigQueryMetriqlBridge
import org.testng.annotations.BeforeSuite
import java.util.UUID

class TestSimpleFilterBigQuery : TestSimpleFilter() {
    override val warehouseBridge = BigQueryMetriqlBridge
    override val testingServer = TestingEnvironmentBigQuery
    override val aq = '`'

    @BeforeSuite
    fun setup() {
        testingServer.init()
        populate()
    }

    override fun populate() {
        val bigQuery = testingServer.createConnection()
        val fields = mutableListOf(
            Field.of("test_int", LegacySQLTypeName.INTEGER),
            Field.of("test_string", LegacySQLTypeName.STRING),
            Field.of("test_double", LegacySQLTypeName.NUMERIC),
            Field.of("test_date", LegacySQLTypeName.DATE),
            Field.of("test_bool", LegacySQLTypeName.BOOLEAN),
            Field.of("test_timestamp", LegacySQLTypeName.TIMESTAMP)
        )
        val schema = Schema.of(fields)
        val tableDefinition = StandardTableDefinition.of(schema)
        val tableId = TableId.of("rakam_test", table)
        val tableInfo = TableInfo.of(tableId, tableDefinition)
        `try?` { bigQuery.getTable(tableId).delete() }
        bigQuery.create(tableInfo)

        val rows = SimpleFilterTests.testInt.mapIndexed { index, i ->
            InsertAllRequest.RowToInsert.of(
                UUID.randomUUID().toString(),
                mapOf(
                    "test_int" to i,
                    "test_string" to SimpleFilterTests.testString[index],
                    "test_double" to SimpleFilterTests.testDouble[index],
                    "test_date" to "${SimpleFilterTests.testDate[index]}",
                    "test_bool" to SimpleFilterTests.testBool[index],
                    "test_timestamp" to "${SimpleFilterTests.testTimestamp[index]}"
                )
            )
        }
        // https://github.com/googleapis/google-cloud-java/issues/3344
        bigQuery.getTable(tableId).insert(rows)
        bigQuery.getTable(tableId).insert(rows)
        bigQuery.getTable(tableId).insert(rows)
        bigQuery.getTable(tableId).insert(rows)
    }
}
