package com.metriql.warehouse.snowflake

import com.metriql.tests.TestingServer
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlExceptions
import com.metriql.warehouse.snowflake.SnowflakeWarehouse.SnowflakeConfig
import net.snowflake.client.jdbc.SnowflakeDriver
import java.lang.IllegalStateException
import java.sql.Connection
import java.sql.SQLException
import java.util.Properties

object TestingEnvironmentSnowflake : TestingServer<Connection> {
    @Volatile
    private var isInitialized = false

    override val config: SnowflakeConfig = JsonHelper.read(
        System.getenv("METRIQL_TEST_SNOWFLAKE_CREDENTIALS")
            ?: throw IllegalStateException("METRIQL_TEST_SNOWFLAKE_CREDENTIALS environment variable is required to run the tests"),
        SnowflakeConfig::class.java
    )

    override val dataSource = SnowflakeDataSource(config)

    private val driver = SnowflakeDriver()

    override fun getQueryRunner(): Connection {
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
        getQueryRunner().use {
            if (!isInitialized) {
                it.createStatement().executeUpdate("""DROP SCHEMA IF EXISTS "DEMO_DB"."RAKAM_TEST" cascade""")
                it.createStatement().execute("""CREATE SCHEMA "DEMO_DB"."RAKAM_TEST" """)
            }
            isInitialized = true
        }
    }
}
