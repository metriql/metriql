package com.metriql.warehouse.bigquery

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import com.metriql.service.dataset.Dataset
import com.metriql.tests.SimpleFilterTests
import com.metriql.tests.TestDataSource
import com.metriql.util.`try?`
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import java.util.UUID
import kotlin.test.assertEquals

class TestDataSourceBigQuery : TestDataSource<BigQuery>() {
    override val useIntsForBoolean = false

    override val testingServer = TestingEnvironmentBigQuery

    open fun init() {}

    @BeforeSuite
    fun setup() {
        testingServer.init()
        init()
        populate()
    }

    @Test
    override fun `test fill defaults`() {
        val datasetTarget = Dataset.Target(Dataset.Target.Type.TABLE, Dataset.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledDatasetTarget = testingServer.dataSource.fillDefaultsToTarget(datasetTarget).value as Dataset.Target.TargetValue.Table
        assertEquals(filledDatasetTarget.database, "rakamui-215316")
        assertEquals(schemaName, filledDatasetTarget.schema)
    }

    @Test
    override fun `test list database names`() {
        val databaseNames = testingServer.dataSource.listDatabaseNames()
        assert(databaseNames.contains(testingServer.dataSource.config.project))
    }

    @Test
    override fun `test listing schema names`() {
        val schemaNames = testingServer.dataSource.listSchemaNames(null)
        assert(schemaNames.contains(testingServer.dataSource.config.dataset))
    }

    override fun populate() {
        val bigQuery = testingServer.getQueryRunner()
        // bigQuery.create(DatasetInfo.of(schemaName))
        val fields = mutableListOf(
            Field.of("test_int", LegacySQLTypeName.INTEGER),
            Field.of("test_string", LegacySQLTypeName.STRING),
            Field.of("test_double", LegacySQLTypeName.NUMERIC),
            Field.of("test_date", LegacySQLTypeName.DATE),
            Field.of("test_bool", LegacySQLTypeName.BOOLEAN),
            Field.of("test_timestamp", LegacySQLTypeName.TIMESTAMP),
            Field.of("test_time", LegacySQLTypeName.TIME)
        )
        val schema = Schema.of(fields)
        val tableDefinition = StandardTableDefinition.of(schema)
        val tableId = TableId.of(schemaName, tableName)
        val tableInfo = TableInfo.of(tableId, tableDefinition)
        `try?` { bigQuery.getTable(tableId).delete() }
        bigQuery.create(tableInfo)

        val rows = SimpleFilterTests.testInt.mapIndexed { index, i ->
            InsertAllRequest.RowToInsert.of(
                UUID.randomUUID().toString(),
                mapOf(
                    "test_int" to testInt[i],
                    "test_string" to testString[index],
                    "test_double" to testDouble[index],
                    "test_date" to "${testDate[index]}",
                    "test_bool" to testBool[index],
                    "test_timestamp" to "${testTimestamp[index]}",
                    "test_time" to "${testTime[index]}"
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
