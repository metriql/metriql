package com.metriql.tests

import com.metriql.warehouse.spi.function.RFunction
import io.trino.spi.type.StandardTypes
import org.testng.annotations.BeforeSuite
import java.sql.Connection
import java.sql.Statement
import java.time.ZoneOffset

abstract class JdbcTestSimpleFilter : TestSimpleFilter<Connection>() {

    @BeforeSuite
    fun setup() {
        testingServer.init()
        populate()
    }

    open fun setTimezone(stmt: Statement) {
        stmt.execute("SET time_zone = 'UTC'")
    }

    fun populate() {
        val mqlTypeMap = testingServer.bridge.mqlTypeMap

        testingServer.getQueryRunner().use { connection ->
            // Create table
            val stmt = connection.createStatement()
            setTimezone(stmt)
            stmt.execute(
                """
                CREATE TABLE ${testingServer.bridge.quoteIdentifier(table)} (
                    test_int ${mqlTypeMap[StandardTypes.INTEGER]},
                    test_string ${mqlTypeMap[StandardTypes.VARCHAR]},
                    test_double ${mqlTypeMap[StandardTypes.DOUBLE]},
                    test_date ${mqlTypeMap[StandardTypes.DATE]},
                    test_bool ${mqlTypeMap[StandardTypes.BOOLEAN]},
                    test_timestamp ${mqlTypeMap[StandardTypes.TIMESTAMP]}
                )
                """.trimIndent()
            )

            // Populate data
            val values = SimpleFilterTests.testInt.mapIndexed { index, i ->
                val requiredFunction = testingServer.bridge.compileFunction(
                    RFunction.FROM_UNIXTIME,
                    listOf(SimpleFilterTests.testTimestamp[index].toEpochSecond(ZoneOffset.UTC))
                )
                """(
                    $i,
                    '${SimpleFilterTests.testString[index]}',
                    ${SimpleFilterTests.testDouble[index]},
                    CAST('${SimpleFilterTests.testDate[index]}' AS ${mqlTypeMap[StandardTypes.DATE]}),
                    ${SimpleFilterTests.testBool[index]},
                    $requiredFunction
                    )
                """.trimIndent()
            }
            stmt.execute(
                """
                INSERT INTO ${testingServer.bridge.quoteIdentifier(table)} (
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
