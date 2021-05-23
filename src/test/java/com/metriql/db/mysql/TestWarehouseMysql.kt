package com.metriql.db.mysql

import com.metriql.model.Model
import com.metriql.tests.TestWarehouse
import com.metriql.warehouse.mysql.MySQLDataSource
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import kotlin.test.assertEquals

class TestWarehouseMysql : TestWarehouse() {
    override val testingServer = TestingEnvironmentMySQL
    override val datasource = MySQLDataSource(testingServer.config)

    @BeforeSuite
    fun setup() {
        testingServer.init()
        populate()
    }

    override fun populate() {
        testingServer.createConnection().use { connection ->
            // Create table
            connection.createStatement().execute(
                """
                CREATE TABLE ${testingServer.getTableReference(tableName)} (
                    test_int INTEGER,
                    test_string TEXT,
                    test_double DOUBLE,
                    test_date DATE,
                    test_bool BOOLEAN,
                    test_timestamp DATETIME,
                    test_time TIME
                )
                """.trimIndent()
            )

            // Populate data
            // FROM_UNIXTIME accepts seconds
            val values = testInt.mapIndexed { index, i ->
                """(
                    $i,
                    '${testString[index]}',
                    ${testDouble[index]},
                    CAST('${testDate[index]}' AS DATE),
                    ${testBool[index]},
                    FROM_UNIXTIME(${testTimestamp[index].toEpochMilli()}/1000),
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

    @Test
    override fun `test generate sql reference`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table("a", "b", "c"))
        val sqlTarget = datasource.sqlReferenceForTarget(modelTarget, "model") { "" }
        assertEquals("`a`.`c` AS `model`", sqlTarget)
    }

    override fun `test fill defaults`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledModelTarget = datasource.fillDefaultsToTarget(modelTarget).value as Model.Target.TargetValue.Table
        assertEquals(filledModelTarget.database, "test_db")
        assertEquals(null, filledModelTarget.schema)
    }

    @Test
    override fun `test list database names`() {
        val databaseNames = datasource.listDatabaseNames()
        assert(databaseNames.contains(datasource.config.database))
    }

    @Test
    override fun `test listing schema names`() {
        assert(true)
    }

    @Test
    override fun `test table schema by sql`() {
        val query = """
        SELECT 1 as test_int,
               'test' as test_string,
               1.1 as test_double,
               CAST('1970-01-01' AS DATE) as test_date,
               TRUE as test_bool,
               CAST('1970-01-01' AS DATETIME) as test_timestamp,
               CAST('15:30:00' AS TIME) as test_time
        """.trimIndent()
        val columns = datasource.getTable(query).columns
        assert(columns.isNotEmpty())
        columns.forEach { column ->
            assert(columnTypes[column.name]?.contains(column.type) == true)
        }
    }
}
