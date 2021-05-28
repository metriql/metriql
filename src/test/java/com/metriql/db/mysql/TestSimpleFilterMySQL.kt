package com.metriql.db.mysql

import com.metriql.tests.SimpleFilterTests
import com.metriql.tests.TestSimpleFilter
import com.metriql.warehouse.mysql.MySQLMetriqlBridge
import org.testng.annotations.BeforeSuite

class TestSimpleFilterMySQL : TestSimpleFilter() {
    override val warehouseBridge = MySQLMetriqlBridge
    override val testingServer = TestingEnvironmentMySQL

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
                CREATE TABLE ${testingServer.getTableReference(table)} (
                    test_int INTEGER,
                    test_string TEXT,
                    test_double FLOAT,
                    test_date DATE,
                    test_bool BOOLEAN,
                    test_timestamp DATETIME
                )
                """.trimIndent()
            )

            // Populate data
            // FROM_UNIXTIME accepts seconds
            val values = SimpleFilterTests.testInt.mapIndexed { index, i ->
                """(
                    $i,
                    '${SimpleFilterTests.testString[index]}',
                    ${SimpleFilterTests.testDouble[index]},
                    CAST('${SimpleFilterTests.testDate[index]}' AS DATE),
                    ${SimpleFilterTests.testBool[index]},
                    FROM_UNIXTIME(${SimpleFilterTests.testTimestamp[index].toEpochMilli()}/1000)
                    )
                """.trimIndent()
            }
            connection.createStatement().execute(
                """
                INSERT INTO ${testingServer.getTableReference(table)} (
                test_int,
                test_string,
                test_double,
                test_date,
                test_bool,
                test_timestamp)
                VALUES ${values.joinToString(", ")}
                """.trimIndent()
            )
        }
    }
}
