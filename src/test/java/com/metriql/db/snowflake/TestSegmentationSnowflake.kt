package com.metriql.db.snowflake

import com.metriql.tests.TestSegmentation
import com.metriql.util.JsonHelper
import com.metriql.warehouse.snowflake.SnowflakeDataSource
import org.testng.annotations.BeforeSuite
import java.sql.Date

class TestSegmentationSnowflake : TestSegmentation() {

    override val testingServer = TestingEnvironmentSnowflake
    override var dataSource = SnowflakeDataSource(testingServer.config)

    @BeforeSuite
    @Throws(Exception::class)
    fun setup() {
        testingServer.init()
        fillData()
    }

    private fun fillData() {
        listOf("_table", "_table2").forEach {
            testingServer.createConnection().use { connection ->
                connection.createStatement().execute("ALTER SESSION SET TIMEZONE = 'UTC'")
                if (it == "_table2") {
                    connection.createStatement().execute(
                        """
                        CREATE TABLE ${testingServer.getTableReference(it)} (
                            teststr VARCHAR,
                            testnumber DOUBLE,
                            testbool  BOOLEAN,
                            testmap OBJECT,
                            testarray  ARRAY,
                            testdate   DATE,
                            _time TIMESTAMP_TZ,
                            testdummy VARCHAR
                        )
                        """.trimIndent()
                    )
                    val values = (0 until SCALE_FACTOR).map { i ->
                        """
                            (
                            'test$i',
                            ${i.toDouble()},
                            ${i % 2 == 0},
                            '${JsonHelper.encode(mapOf("test$i" to i.toDouble()))}',
                            ${i.toDouble()},
                            CAST('${Date(i.toLong())}' AS DATE),
                            ${(i.toLong() * 60 * 60 * 24)},
                            'test$i'
                            )
                        """.trimIndent()
                    }
                    val insertQuery = """
                    INSERT INTO ${testingServer.getTableReference(it)}
                    SELECT column1, column2, column3, parse_json(column4), array_construct(column5), column6, TO_TIMESTAMP_NTZ(column7), column8
                    FROM VALUES
                    ${values.joinToString(", ")}
                    """.trimMargin()
                    connection.createStatement().executeUpdate(insertQuery)
                } else {
                    connection.createStatement().execute(
                        """
                        CREATE TABLE ${testingServer.getTableReference(it)} (
                            teststr VARCHAR,
                            testnumber DOUBLE,
                            testbool  BOOLEAN,
                            testmap   VARIANT,
                            testarray  ARRAY,
                            testdate   DATE,
                            _time TIMESTAMP_TZ
                        )
                        """.trimIndent()
                    )
                    val values = (0 until SCALE_FACTOR).map { i ->
                        """
                            (
                            'test$i',
                            ${i.toDouble()},
                            ${i % 2 == 0},
                            '${JsonHelper.encode(mapOf("test$i" to i.toDouble()))}',
                            ${i.toDouble()},
                            CAST('${Date(i.toLong())}' AS DATE),
                            ${(i.toLong() * 60 * 60 * 24)}
                            )
                        """.trimIndent()
                    }
                    val insertQuery = """
                    INSERT INTO ${testingServer.getTableReference(it)}
                    SELECT column1, column2, column3, parse_json(column4), array_construct(column5), column6, TO_TIMESTAMP_NTZ(column7)
                    FROM VALUES
                    ${values.joinToString(", ")}
                    """.trimMargin()
                    connection.createStatement().executeUpdate(insertQuery)
                }
            }
        }
    }
}
