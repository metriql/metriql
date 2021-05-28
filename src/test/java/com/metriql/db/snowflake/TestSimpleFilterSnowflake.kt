package com.metriql.db.snowflake

import com.metriql.tests.SimpleFilterTests
import com.metriql.tests.TestSimpleFilter
import com.metriql.warehouse.snowflake.SnowflakeMetriqlBridge
import org.testng.annotations.BeforeSuite

class TestSimpleFilterSnowflake : TestSimpleFilter() {
    override val warehouseBridge = SnowflakeMetriqlBridge
    override val testingServer = TestingEnvironmentSnowflake

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
                    "test_int" INTEGER,
                    "test_string" VARCHAR,
                    "test_double" DOUBLE,
                    "test_date" DATE,
                    "test_bool" BOOLEAN,
                    "test_timestamp" TIMESTAMP_TZ
                )
                """.trimIndent()
            )
        }

        // Populate data
        testingServer.createConnection().use {
            val values = SimpleFilterTests.testInt.mapIndexed { index, i ->
                """(
                    $i,
                    '${SimpleFilterTests.testString[index]}',
                    ${SimpleFilterTests.testDouble[index]},
                    CAST('${SimpleFilterTests.testDate[index]}' AS DATE),
                    ${SimpleFilterTests.testBool[index]},
                    '${SimpleFilterTests.testTimestamp[index]}'
                    )
                """.trimIndent()
            }
            val insertQuery = """
                    INSERT INTO ${testingServer.getTableReference(table)}
                    SELECT column1, column2, column3, column4, column5, TO_TIMESTAMP_TZ(column6)
                    FROM VALUES
                    ${values.joinToString(", ")}
                    """.trimMargin()
            it.createStatement().executeUpdate(insertQuery)
        }
    }
}
