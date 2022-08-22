package com.metriql.tests

import com.metriql.db.FieldType
import com.metriql.service.model.Model
import io.trino.spi.type.StandardTypes
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
import java.sql.Connection
import kotlin.test.assertEquals

abstract class JdbcTestDataSource(override val useIntsForBoolean: Boolean = false) : TestDataSource<Connection>() {

    open fun init() {}

    @BeforeSuite
    fun setup() {
        testingServer.init()
        init()
        populate()
    }

    open val tableDefinition: String = ""

    override fun populate() {
        testingServer.getQueryRunner().use { connection ->
            // Create table
            connection.createStatement().execute(
                """
                CREATE TABLE ${testingServer.bridge.quoteIdentifier(tableName)} (
                    test_int ${testingServer.bridge.mqlTypeMap[StandardTypes.INTEGER]},
                    test_string ${testingServer.bridge.mqlTypeMap[StandardTypes.VARCHAR]},
                    test_double ${testingServer.bridge.mqlTypeMap[StandardTypes.DOUBLE]},
                    test_date ${testingServer.bridge.mqlTypeMap[StandardTypes.DATE]},
                    test_bool ${testingServer.bridge.mqlTypeMap[StandardTypes.BOOLEAN]},
                    test_timestamp ${testingServer.bridge.mqlTypeMap[StandardTypes.TIMESTAMP]},
                    test_time ${testingServer.bridge.mqlTypeMap[StandardTypes.TIME]}
                ) $tableDefinition
                """.trimIndent()
            )

            // Populate data
            val values = testInt.mapIndexed { index, i ->
                """(
                    $i,
                    '${testString[index]}',
                    ${testDouble[index]},
                    ${testingServer.bridge.filters.parseAnyValue(testDate[index], context, FieldType.DATE)},
                    ${testBool[index]},
                    ${testingServer.bridge.filters.parseAnyValue(testTimestamp[index], context, FieldType.TIMESTAMP)},
                    ${testingServer.bridge.filters.parseAnyValue(testTime[index], context, FieldType.TIME)}
                    )
                """.trimIndent()
            }
            connection.createStatement().execute(
                """
                INSERT INTO ${testingServer.bridge.quoteIdentifier(tableName)} (
                test_int,
                test_string,
                test_double,
                test_date,
                test_bool,
                test_timestamp,
                test_time)
                VALUES ${values.joinToString(", ")}
                """.trimIndent()
            )
        }
    }

    @Test
    override fun `test list database names`() {
        val databaseNames = testingServer.dataSource.listDatabaseNames()
        assert(databaseNames.contains(testingServer.dataSource.config.warehouseDatabase()))
    }

    @Test
    override fun `test listing schema names`() {
        val schemaNames = testingServer.dataSource.listSchemaNames(null)
        assert(schemaNames.contains(testingServer.dataSource.config.warehouseSchema() ?: "public"))
    }
}
