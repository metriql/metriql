package com.metriql.warehouse.mysql

import com.metriql.tests.TestSegmentation
import org.testng.annotations.BeforeSuite
import java.sql.Date

class TestSegmentationMySQL : TestSegmentation() {
    override val testingServer = TestingEnvironmentMySQL

    // Services
    override var dataSource = MySQLDataSource(testingServer.config)

    @BeforeSuite
    @Throws(Exception::class)
    fun setup() {
        testingServer.init()
        fillData()
    }

    private fun fillData() {
        listOf("_table", "_table2").forEach {
            testingServer.getQueryRunner().use { connection ->
                connection.createStatement().execute("set time_zone = '+00:00'")
                if (it == "_table2") {
                    connection.createStatement().execute(
                        """
                        CREATE TABLE ${testingServer.bridge.quoteIdentifier(it)} (
                            teststr TEXT,
                            testnumber DOUBLE,
                            testbool  BOOLEAN,
                            testdate   DATE,
                            _time DATETIME,
                            testdummy VARCHAR(55)
                        )
                        """.trimIndent()
                    )
                    val values = (0 until SCALE_FACTOR).map { i ->
                        """
                            (
                            'test$i',
                            ${i.toDouble()},
                            ${i % 2 == 0},
                            CAST('${Date(i.toLong())}' AS DATE),
                            FROM_UNIXTIME(${(i.toLong() * 60 * 60 * 24) + (if (i == 0) 1 else 0)}),
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
                            teststr TEXT,
                            testnumber DOUBLE,
                            testbool  BOOLEAN,
                            testdate   DATE,
                            _time DATETIME
                        )
                        """.trimIndent()
                    )
                    val values = (0 until SCALE_FACTOR).map { i ->
                        """
                            (
                            'test$i',
                            ${i.toDouble()},
                            ${i % 2 == 0},
                            CAST('${Date(i.toLong())}' AS DATE),
                            FROM_UNIXTIME(${(i.toLong() * 60 * 60 * 24) + (if (i == 0) 1 else 0)})
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
