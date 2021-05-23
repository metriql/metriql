package com.metriql.db.snowflake

import com.metriql.model.Model
import com.metriql.tests.TestWarehouse
import com.metriql.warehouse.snowflake.SnowflakeDataSource
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import kotlin.test.assertEquals

class TestWarehouseSnowflake : TestWarehouse() {
    override val testingServer = TestingEnvironmentSnowflake
    override val datasource = SnowflakeDataSource(testingServer.config)

    @BeforeSuite
    fun setup() {
        testingServer.init()
        populate()
    }

    override fun populate() {
        testingServer.createConnection().use {
            // Create table
            it.createStatement().execute(
                """
                CREATE TABLE ${testingServer.getTableReference(tableName)} (
                    "test_int" INTEGER,
                    "test_string" VARCHAR,
                    "test_double" DOUBLE,
                    "test_date" DATE,
                    "test_bool" BOOLEAN,
                    "test_timestamp" TIMESTAMP_TZ,
                    "test_time" TIME
                )
                """.trimIndent()
            )
        }

        // Populate data
        testingServer.createConnection().use {
            val values = testInt.mapIndexed { index, i ->
                """(
                    $i,
                    '${testString[index]}',
                    ${testDouble[index]},
                    CAST('${testDate[index]}' AS DATE),
                    ${testBool[index]},
                    '${testTimestamp[index]}',
                    CAST('${testTime[index]}' AS TIME)
                    )
                """.trimIndent()
            }
            val insertQuery = """
                    INSERT INTO ${testingServer.getTableReference(tableName)}
                    SELECT column1, column2, column3, column4, column5, TO_TIMESTAMP_TZ(column6), column7
                    FROM VALUES
                    ${values.joinToString(", ")}
                    """.trimMargin()
            it.createStatement().executeUpdate(insertQuery)
        }
    }

    @Test
    override fun `test fill defaults`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledModelTarget = datasource.fillDefaultsToTarget(modelTarget).value as Model.Target.TargetValue.Table
        assertEquals(filledModelTarget.database, "DEMO_DB")
        assertEquals("RAKAM_TEST", filledModelTarget.schema)
    }

    @Test
    override fun `test list database names`() {
        val databaseNames = datasource.listDatabaseNames()
        assert(databaseNames.contains(datasource.config.database))
    }

    @Test
    override fun `test listing schema names`() {
        val schemaNames = datasource.listSchemaNames(null)
        assert(schemaNames.contains(datasource.config.schema))
    }

    @Test
    override fun `test table schema by sql`() {
        val query = """
        SELECT 1 as "test_int",
               'test' as "test_string",
               CAST(1.1 AS DOUBLE) as "test_double",
               CAST('1970-01-01' AS DATE) as "test_date",
               TRUE as "test_bool",
               CAST('1970-01-01' AS TIMESTAMP) as "test_timestamp",
               CAST('15:30:00' AS TIME) as "test_time"
        """.trimIndent()
        datasource.getTable(query).columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
        assert(true)
    }
}
