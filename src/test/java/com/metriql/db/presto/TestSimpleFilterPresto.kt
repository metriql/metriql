package com.metriql.db.presto

import com.metriql.tests.SimpleFilterTests.testBool
import com.metriql.tests.SimpleFilterTests.testDate
import com.metriql.tests.SimpleFilterTests.testDouble
import com.metriql.tests.SimpleFilterTests.testInt
import com.metriql.tests.SimpleFilterTests.testString
import com.metriql.tests.SimpleFilterTests.testTimestamp
import com.metriql.tests.TestSimpleFilter
import com.metriql.warehouse.presto.PrestoDataSource
import org.testng.annotations.BeforeSuite

class TestSimpleFilterPresto : TestSimpleFilter() {
    override val testingServer = TestingEnvironmentPresto
    override val dataSource = PrestoDataSource(testingServer.config)

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
                CREATE TABLE ${testingServer.getTableReference(table)} (
                    test_int INTEGER,
                    test_string VARCHAR,
                    test_double DOUBLE,
                    test_date DATE,
                    test_bool BOOLEAN,
                    test_timestamp TIMESTAMP WITH TIME ZONE
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
                    from_iso8601_timestamp('${testTimestamp[index]}')
                    )
                """.trimIndent()
            }
            it.createStatement().execute(
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
