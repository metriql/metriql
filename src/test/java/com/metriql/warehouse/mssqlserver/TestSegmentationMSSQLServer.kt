package com.metriql.warehouse.mssqlserver

import com.metriql.tests.TestSegmentation
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import java.sql.Date

// TODO: fix timezone issues first
@Test(enabled = false)
class TestSegmentationMSSQLServer : TestSegmentation() {
    val testingServer = TestingEnvironmentMSSQLServer
    // Services
    override val dataSource = MSSQLDataSource(testingServer.config)

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
                            teststr VARCHAR(255),
                            testnumber FLOAT,
                            testbool  INT,
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
                            ${if (i % 2 == 0) 1 else 0},
                            CAST('${Date(i.toLong())}' AS DATE),
                            DATEADD(s, ${(i.toLong() * 60 * 60 * 24) / 1000}, '19700101'),
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
                            teststr VARCHAR(255),
                            testnumber FLOAT,
                            testbool  INT,
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
                            ${if (i % 2 == 0) 1 else 0},
                            CAST('${Date(i.toLong())}' AS DATE),
                            DATEADD(s, ${(i.toLong() * 60 * 60 * 24) / 1000}, '19700101')
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
