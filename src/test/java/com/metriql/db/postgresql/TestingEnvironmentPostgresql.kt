package com.metriql.db.postgresql

import com.metriql.tests.TestingServer
import com.metriql.util.ValidationUtil.quoteIdentifier
import com.metriql.util.`try?`
import com.metriql.warehouse.postgresql.PostgresqlDataSource
import com.metriql.warehouse.postgresql.PostgresqlWarehouse
import org.testcontainers.containers.PostgreSQLContainer
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres
import java.sql.Connection
import java.sql.ResultSet

object TestingEnvironmentPostgresql : TestingServer<EmbeddedPostgres, Connection>() {
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

    override fun getTableReference(tableName: String): String {
        return "${quoteIdentifier(config.schema ?: "public")}.${quoteIdentifier(tableName)}"
    }

    override fun createConnection(): Connection {
        return PostgresqlDataSource(config, readOnly = false).openConnection()
    }

    @Synchronized
    override fun init() {
        createConnection().use { conn ->
            `try?` { conn.createStatement().executeUpdate("DROP SCHEMA ${config.schema ?: "public"} cascade") }
            conn.createStatement().executeUpdate("CREATE SCHEMA ${config.schema ?: "public"}")
        }
    }

    override fun resultSetFor(query: String): ResultSet {
        createConnection().use {
            return it.createStatement().executeQuery(query)
        }
    }
}
