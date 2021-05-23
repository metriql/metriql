package com.metriql.db.postgresql

import com.metriql.model.Model
import com.metriql.tests.TestWarehouse
import com.metriql.warehouse.postgresql.PostgresqlDataSource
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import kotlin.test.assertEquals

class TestWarehousePostgresql : TestWarehouse() {
    override val testingServer = TestingEnvironmentPostgresql
    override val datasource = PostgresqlDataSource(testingServer.config)

    override fun populate() {
        testingServer.createConnection().use { connection ->
            // Create table
            connection.createStatement().execute(
                """
                CREATE TABLE ${testingServer.getTableReference(tableName)} (
                    test_int INTEGER,
                    test_string VARCHAR,
                    test_double FLOAT,
                    test_date DATE,
                    test_bool BOOLEAN,
                    test_timestamp TIMESTAMP WITH TIME ZONE,
                    test_time TIME
                )
                """.trimIndent()
            )

            // Populate data
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
            connection.createStatement().execute(
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

    @BeforeSuite
    fun setup() {
        testingServer.init()
        populate()
    }

    @Test
    override fun `test generate sql reference`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table("a", "b", "c"))
        val sqlTarget = datasource.sqlReferenceForTarget(modelTarget, "model") { "" }
        assertEquals("\"b\".\"c\" AS \"model\"", sqlTarget)
    }

    @Test
    override fun `test list database names`() {
        val databaseNames = datasource.listDatabaseNames()
        assert(databaseNames.contains(datasource.config.dbname))
    }

    @Test
    override fun `test listing schema names`() {
        val schemaNames = datasource.listSchemaNames(null)
        assert(schemaNames.contains(datasource.config.schema))
    }
}
