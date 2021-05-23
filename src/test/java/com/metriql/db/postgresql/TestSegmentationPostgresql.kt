package com.metriql.db.postgresql

import com.google.common.collect.ImmutableMap
import com.metriql.tests.TestSegmentation
import com.metriql.util.JsonHelper
import com.metriql.warehouse.postgresql.PostgresqlDataSource
import org.postgresql.util.PGobject
import org.testng.annotations.BeforeSuite
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.Instant

class TestSegmentationPostgresql : TestSegmentation() {
    override val testingServer = TestingEnvironmentPostgresql

    // Services
    override var dataSource = PostgresqlDataSource(testingServer.config)

    @BeforeSuite
    @Throws(Exception::class)
    fun setup() {
        testingServer.init()
        fillData()
    }

    private fun fillData() {
        listOf("_table", "_table2").forEach {
            testingServer.createConnection().use { connection ->
                val statement: PreparedStatement
                if (it == "_table2") {
                    connection.createStatement().executeUpdate(
                        """
                        CREATE TABLE ${testingServer.getTableReference(it)} (
                            teststr text,
                            testnumber float(8),
                            testbool  bool,
                            testmap   jsonb,
                            testarray  float(8)[],
                            testdate   date,
                            _time timestamp with time zone,
                            testdummy text
                        );
                        """.trimIndent()
                    )
                    statement =
                        connection.prepareStatement(
                            "INSERT INTO ${testingServer.getTableReference(it)} " +
                                "(teststr, testnumber, testbool, testmap, testarray, testdate, _time, testdummy) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                        )
                } else {
                    connection.createStatement().executeUpdate(
                        """
                        CREATE TABLE ${testingServer.getTableReference(it)} (
                            teststr text,
                            testnumber float(8),
                            testbool  bool,
                            testmap   jsonb,
                            testarray  float(8)[],
                            testdate   date,
                            _time timestamp with time zone
                        );
                        """.trimIndent()
                    )
                    statement =
                        connection.prepareStatement(
                            "INSERT INTO ${testingServer.getTableReference(it)} (teststr, testnumber, testbool, testmap, testarray, testdate, _time) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?)"
                        )
                }

                for (i in 0 until SCALE_FACTOR) {
                    statement.setString(1, "test$i")
                    statement.setDouble(2, i.toDouble())
                    statement.setBoolean(3, i % 2 == 0)

                    val jsonObject = PGobject()
                    jsonObject.type = "jsonb"
                    jsonObject.value = JsonHelper.encode(ImmutableMap.of("test$i", i.toDouble()))
                    statement.setObject(4, jsonObject)

                    statement.setArray(5, connection.createArrayOf("float", arrayOf(java.lang.Double.valueOf(i.toDouble()))))
                    statement.setDate(6, Date(i.toLong()))
                    statement.setTimestamp(7, Timestamp.from(Instant.ofEpochMilli((i.toLong() * 1000 * 60 * 60 * 24))))
                    if (it == "_table2") {
                        statement.setString(8, "test$i")
                    }
                    statement.addBatch()
                }

                statement.executeBatch()
            }
        }
    }
}
