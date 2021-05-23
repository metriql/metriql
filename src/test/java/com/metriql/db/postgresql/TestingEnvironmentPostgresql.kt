package com.metriql.db.postgresql

import com.metriql.DockerContainer
import com.metriql.db.TestingServer
import com.metriql.util.ValidationUtil.quoteIdentifier
import com.metriql.util.`try?`
import com.metriql.warehouse.postgresql.PostgresqlDataSource
import com.metriql.warehouse.postgresql.PostgresqlWarehouse
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

object TestingEnvironmentPostgresql : TestingServer<EmbeddedPostgres, Connection>() {
    private const val PG_USER = "metriql"
    private const val PG_ROOT_PASSWORD = "mysqlpassword"
    private const val PG_DATABASE = "metriql"
    private const val PG_PORT = 5432

    private val dockerContainer: DockerContainer by lazy(mode = LazyThreadSafetyMode.SYNCHRONIZED) {
        DockerContainer(
            "postgres:9.6.22",
            listOf(PG_PORT),
            mapOf(
                "POSTGRES_PASSWORD" to PG_ROOT_PASSWORD,
                "POSTGRES_USER" to PG_USER,
                "POSTGRES_DB" to PG_DATABASE
            )
        ) {
            runQueryAsRoot(it, "SELECT 1")
        }
    }

    override val config by lazy {
        PostgresqlWarehouse.PostgresqlConfig(
            "localhost",
            dockerContainer.getHostPort(PG_PORT),
            PG_DATABASE,
            "rakam_test",
            PG_USER,
            PG_ROOT_PASSWORD,
            usePool = false
        )
    }

    private fun runQueryAsRoot(hostPortProvider: DockerContainer.HostPortProvider, query: String) {
        DriverManager.getConnection(getJdbcUrl(hostPortProvider, PG_USER, PG_ROOT_PASSWORD)).use { conn -> conn.createStatement().use { stmt -> stmt.execute(query) } }
    }

    private fun getJdbcUrl(hostPortProvider: DockerContainer.HostPortProvider, user: String, password: String): String? {
        return "jdbc:postgresql://localhost:${hostPortProvider.getHostPort(PG_PORT)}/$PG_DATABASE?user=$user&password=$password"
    }

    override fun getTableReference(tableName: String): String {
        return "${quoteIdentifier(config.schema!!)}.${quoteIdentifier(tableName)}"
    }

    override fun createConnection(): Connection {
        return PostgresqlDataSource(config, readOnly = false).openConnection()
    }

    @Synchronized
    override fun init() {
        createConnection().use { conn ->
            `try?` { conn.createStatement().executeUpdate("DROP SCHEMA ${config.schema} cascade") }
            conn.createStatement().executeUpdate("CREATE SCHEMA ${config.schema}")
        }
    }

    override fun resultSetFor(query: String): ResultSet {
        createConnection().use {
            return it.createStatement().executeQuery(query)
        }
    }
}
