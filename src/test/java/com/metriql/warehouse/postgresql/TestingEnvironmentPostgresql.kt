package com.metriql.warehouse.postgresql

import com.metriql.tests.TestingServer
import com.metriql.util.`try?`
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.Connection

object TestingEnvironmentPostgresql : TestingServer<Connection> {
    private const val PG_USER = "metriql"
    private const val PG_ROOT_PASSWORD = "mysqlpassword"
    private const val PG_DATABASE = "metriql"
    private const val PG_PORT = 5432

    private val dockerContainer: PostgreSQLContainer<Nothing> by lazy(mode = LazyThreadSafetyMode.SYNCHRONIZED) {
        val server = PostgreSQLContainer<Nothing>("postgres:9.6.22").apply {
            withPassword(PG_ROOT_PASSWORD)
            withUsername(PG_USER)
            withDatabaseName(PG_DATABASE)
        }
        server.start()
        server
    }

    override val config by lazy {
        PostgresqlWarehouse.PostgresqlConfig(
            "localhost",
            dockerContainer.getMappedPort(PG_PORT),
            PG_DATABASE,
            "rakam_test",
            PG_USER,
            PG_ROOT_PASSWORD,
            usePool = false
        )
    }

    override val dataSource = PostgresqlDataSource(config)

    override fun getQueryRunner() = dataSource.openConnection()

    @Synchronized
    override fun init() {
        getQueryRunner().use { conn ->
            `try?` { conn.createStatement().executeUpdate("DROP SCHEMA ${config.schema ?: "public"} cascade") }
            conn.createStatement().executeUpdate("CREATE SCHEMA ${config.schema ?: "public"}")
        }
    }
}
