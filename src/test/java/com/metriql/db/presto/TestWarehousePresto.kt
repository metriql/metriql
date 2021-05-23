package com.metriql.db.presto

import com.metriql.model.Model
import com.metriql.tests.TestWarehouse
import com.metriql.warehouse.presto.PrestoDataSource
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import kotlin.test.assertEquals

class TestWarehousePresto : TestWarehouse() {
    override val testingServer = TestingEnvironmentPresto
    override val datasource = PrestoDataSource(testingServer.config)

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
                    test_int INTEGER,
                    test_string VARCHAR,
                    test_double DOUBLE,
                    test_date DATE,
                    test_bool BOOLEAN,
                    test_timestamp TIMESTAMP WITH TIME ZONE,
                    test_time TIME
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
                    from_iso8601_timestamp('${testTimestamp[index]}'),
                    CAST('${testTime[index]}' AS TIME)
                    )
                """.trimIndent()
            }
            it.createStatement().execute(
                """
                INSERT INTO ${testingServer.getTableReference(tableName)} (
                test_int,
                test_string,
                test_double,
                test_date,
                test_bool,
                test_timestamp,
                test_time)
                VALUES ${values.joinToString(", ")}
                """.trimIndent()
            )
        }
    }

    @Test
    override fun `test fill defaults`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledModelTarget = datasource.fillDefaultsToTarget(modelTarget).value as Model.Target.TargetValue.Table
        assertEquals(filledModelTarget.database, "memory")
        assertEquals("rakam_test", filledModelTarget.schema)
    }

    @Test
    override fun `test list database names`() {
        val databaseNames = datasource.listDatabaseNames()
        assert(databaseNames.contains(datasource.config.catalog))
    }

    @Test
    override fun `test listing schema names`() {
        val schemaNames = datasource.listSchemaNames(null)
        assert(schemaNames.contains(datasource.config.schema))
    }
}
