package com.metriql.db.mysql

import com.metriql.tests.SimpleFilterTests
import com.metriql.tests.TestSimpleFilter
import com.metriql.warehouse.mysql.MySQLDataSource
import org.testng.annotations.BeforeSuite
import java.time.ZoneOffset

class TestSimpleFilterMySQL : TestSimpleFilter() {
    override val testingServer = TestingEnvironmentMySQL
    override val dataSource = MySQLDataSource(testingServer.config)

    @BeforeSuite
    fun setup() {
        testingServer.init()
        populate()
    }

    override fun populate() {
        testingServer.createConnection().use { connection ->
            // Create table
            val stmt = connection.createStatement()
            stmt.execute("SET time_zone = 'UTC'")
            stmt.execute(
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
                    FROM_UNIXTIME(${SimpleFilterTests.testTimestamp[index].toEpochSecond(ZoneOffset.UTC)})
                    )
                """.trimIndent()
            }
            stmt.execute(
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
