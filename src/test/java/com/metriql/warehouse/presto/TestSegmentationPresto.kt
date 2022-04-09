package com.metriql.warehouse.presto

import com.metriql.tests.TestSegmentation
import com.metriql.util.JsonHelper
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import java.sql.Date
import java.time.Instant

// implement presto timeframes first
@Test(enabled = false)
class TestSegmentationPresto : TestSegmentation() {

    override val testingServer = TestingEnvironmentPresto

    // Services
    override var dataSource = PrestoDataSource(testingServer.config)

    @BeforeSuite
    @Throws(Exception::class)
    fun setup() {
        testingServer.init()
        fillData()
    }

    private fun fillData() {
        listOf("_table", "_table2").forEach {
            testingServer.getQueryRunner().use { connection ->
                if (it == "_table2") {
                    connection.createStatement().execute(
                        """
                        CREATE TABLE ${testingServer.bridge.quoteIdentifier(it)} (
                            teststr VARCHAR,
                            testnumber DOUBLE,
                            testbool  BOOLEAN,
                            testmap   JSON,
                            testarray  ARRAY<DOUBLE>,
                            testdate   DATE,
                            _time TIMESTAMP WITH TIME ZONE,
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
                            JSON '${JsonHelper.encode(mapOf("test$i" to i.toDouble()))}',
                            ARRAY [${i.toDouble()}],
                            CAST('${Date(i.toLong())}' AS DATE),
                            from_iso8601_timestamp('${Instant.ofEpochMilli((i.toLong() * 1000 * 60 * 60 * 24))}'),
                            'test$i'
                            )
                        """.trimIndent()
                    }
                    val insertQuery = "INSERT INTO ${testingServer.bridge.quoteIdentifier(it)} VALUES ${values.joinToString(", ")}"
                    connection.createStatement().executeUpdate(insertQuery)
                } else {
                    connection.createStatement().execute(
                        """
                        CREATE TABLE ${testingServer.bridge.quoteIdentifier(it)} (
                            teststr VARCHAR,
                            testnumber DOUBLE,
                            testbool  BOOLEAN,
                            testmap   JSON,
                            testarray  ARRAY<DOUBLE>,
                            testdate   DATE,
                            _time TIMESTAMP WITH TIME ZONE
                        )
                        """.trimIndent()
                    )
                    val values = (0 until SCALE_FACTOR).map { i ->
                        """
                            (
                            'test$i',
                            ${i.toDouble()},
                            ${i % 2 == 0},
                            JSON '${JsonHelper.encode(mapOf("test$i" to i.toDouble()))}',
                            ARRAY [${i.toDouble()}],
                            CAST('${Date(i.toLong())}' AS DATE),
                            from_iso8601_timestamp('${Instant.ofEpochMilli((i.toLong() * 1000 * 60 * 60 * 24))}')
                            )
                        """.trimIndent()
                    }
                    val insertQuery = "INSERT INTO ${testingServer.bridge.quoteIdentifier(it)} VALUES ${values.joinToString(", ")}"
                    connection.createStatement().executeUpdate(insertQuery)
                }
            }
        }
    }
}
