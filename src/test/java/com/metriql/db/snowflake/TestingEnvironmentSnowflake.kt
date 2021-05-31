package com.metriql.db.snowflake

import com.metriql.db.TestingServer
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlExceptions
import com.metriql.warehouse.snowflake.SnowflakeWarehouse.SnowflakeConfig
import net.snowflake.client.jdbc.SnowflakeDriver
import java.lang.IllegalStateException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.util.Properties

object TestingEnvironmentSnowflake : TestingServer<Unit, Connection>() {

    override val config = JsonHelper.read(
        System.getenv("METRIQL_TEST_SNOWFLAKE_CREDENTIALS")
            ?: throw IllegalStateException("METRIQL_TEST_SNOWFLAKE_CREDENTIALS environment variable is required to run the tests"),
        SnowflakeConfig::class.java
    )

    override fun getTableReference(tableName: String): String {
        return "${config.schema}.$tableName"
    }

    private val driver = SnowflakeDriver()

    @Volatile
    private var isInitialized = false

    override fun createConnection(): Connection {
        val properties = Properties()
        properties["user"] = config.user
        properties["password"] = config.password
        properties["db"] = config.database
        properties["schema"] = config.schema

        return try {
            driver.connect("jdbc:snowflake://${config.account}.${config.regionId}.snowflakecomputing.com", properties)
        } catch (e: SQLException) {
            throw MetriqlExceptions.SYSTEM_EXCEPTION_FROM_CAUSE.exceptionFromObject(e)
        }
    }

    override fun init() {
        createConnection().use {
            if (!isInitialized) {
                it.createStatement().executeUpdate("""DROP SCHEMA IF EXISTS "DEMO_DB"."RAKAM_TEST" cascade""")
                it.createStatement().execute("""CREATE SCHEMA "DEMO_DB"."RAKAM_TEST" """)
            }
            isInitialized = true
        }
    }

    override fun resultSetFor(query: String): ResultSet {
        createConnection().use {
            return it.createStatement().executeQuery(query)
        }
    }
}
