package com.metriql.db.mssqlserver

import com.metriql.service.model.Model
import com.metriql.tests.TestWarehouse
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import kotlin.test.assertEquals

class TestWarehouseMSSQLServer : TestWarehouse() {
    override val testingServer = TestingEnvironmentMSSQLServer
    override val datasource = testingServer.dataSource

    override fun populate() {
        testingServer.createConnection().use { connection ->
            // Create table
            connection.createStatement().execute(
                """
                CREATE TABLE ${testingServer.getTableReference(tableName)} (
                    test_int INTEGER,
                    test_string VARCHAR(55),
                    test_double FLOAT,
                    test_date DATE,
                    test_bool BIT,
                    test_timestamp datetimeoffset,
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
                    ${if (testBool[index]) 1 else 0},
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
    override fun `test table schema by sql`() {
        val query = """
        SELECT 1 as test_int,
               'test' as test_string,
               1.1 as test_double,
               CAST('1970-01-01' AS DATE) as test_date,
               1 as test_bool,
               CAST('1970-01-01' AS DATETIME) as test_timestamp,
               CAST('15:30:00' AS TIME) as test_time
        """.trimIndent()
        datasource.getTable(query).columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
        assert(true)
    }

    @Test
    override fun `test fill defaults`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledModelTarget = datasource.fillDefaultsToTarget(modelTarget).value as Model.Target.TargetValue.Table
        assertEquals(filledModelTarget.database, "employee")
        assertEquals("rakam_test", filledModelTarget.schema)
    }

    @Test
    override fun `test generate sql reference`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table("a", "b", "c"))
        val sqlTarget = datasource.sqlReferenceForTarget(modelTarget, "model") { "" }
        assertEquals("\"a\".\"b\".\"c\" AS \"model\"", sqlTarget)
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
}
