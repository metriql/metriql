package com.metriql.db.bigquery

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import com.metriql.service.model.Model
import com.metriql.tests.SimpleFilterTests
import com.metriql.tests.TestWarehouse
import com.metriql.util.`try?`
import com.metriql.warehouse.bigquery.BigQueryDataSource
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import java.util.UUID
import kotlin.test.assertEquals

class TestWarehouseBigQuery : TestWarehouse() {

    override val testingServer = TestingEnvironmentBigQuery
    override val datasource = BigQueryDataSource(testingServer.config)

    @BeforeSuite
    fun setup() {
        testingServer.init()
        populate()
    }

    @Test
    override fun `test generate sql reference`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table("a", "b", "c"))
        val sqlTarget = datasource.sqlReferenceForTarget(modelTarget, "model") { "" }
        assertEquals("`a`.`b`.`c` AS `model`", sqlTarget)
    }

    @Test
    override fun `test fill defaults`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledModelTarget = datasource.fillDefaultsToTarget(modelTarget).value as Model.Target.TargetValue.Table
        assertEquals(filledModelTarget.database, "rakamui-215316")
        assertEquals(schemaName, filledModelTarget.schema)
    }

    @Test
    override fun `test list database names`() {
        val databaseNames = datasource.listDatabaseNames()
        assert(databaseNames.contains(datasource.config.project))
    }

    @Test
    override fun `test listing schema names`() {
        val schemaNames = datasource.listSchemaNames(null)
        assert(schemaNames.contains(datasource.config.dataset))
    }

    override fun populate() {
        val bigQuery = testingServer.createConnection()
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
