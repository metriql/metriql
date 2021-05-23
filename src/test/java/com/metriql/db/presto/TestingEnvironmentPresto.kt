package com.metriql.db.presto

import com.facebook.presto.jdbc.PrestoConnection
import com.metriql.DockerContainer
import com.metriql.db.TestingServer
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.presto.PrestoWarehouse
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.Properties

object TestingEnvironmentPresto : TestingServer<Unit, PrestoConnection>() {
    private const val PRESTO_HOST = "127.0.0.1"
    private const val PRESTO_CATALOG = "memory"
    private const val PRESTO_USER = "metriql_user"
    private const val PRESTO_PORT = 8080

    private val dockerContainer: DockerContainer by lazy(mode = LazyThreadSafetyMode.SYNCHRONIZED) {
        DockerContainer(
            "starburstdata/presto:350-e.2",
            listOf(PRESTO_PORT),
            mapOf()
        ) {
            runQueryAsRoot(it, "SELECT 1")
        }
    }

    private fun runQueryAsRoot(hostPortProvider: DockerContainer.HostPortProvider, query: String) {
        DriverManager.getConnection(getJdbcUrl(hostPortProvider)).use { conn -> conn.createStatement().use { stmt -> stmt.execute(query) } }
    }

    private fun getJdbcUrl(hostPortProvider: DockerContainer.HostPortProvider): String? {
        return "jdbc:presto://localhost:${hostPortProvider.getHostPort(PRESTO_PORT)}/$PRESTO_CATALOG?user=$PRESTO_USER"
    }

    override val config = PrestoWarehouse.PrestoConfig(
        PRESTO_HOST,
        PRESTO_PORT,
        PRESTO_CATALOG,
        "rakam_test",
        PRESTO_USER,
        ""
    )

    override fun getTableReference(tableName: String): String {
        return "${ValidationUtil.quoteIdentifier(config.schema)}.${ValidationUtil.quoteIdentifier(tableName)}"
    }

    override fun createConnection(): PrestoConnection {
        val properties = Properties().apply {
            setProperty("SSL", "false")
        }

        val connection = DriverManager.getConnection(getJdbcUrl(dockerContainer::getHostPort), properties)
        return connection.unwrap(PrestoConnection::class.java)
    }

    @Synchronized
    override fun init() {
        createConnection().use {
            val stmt = it.createStatement()
            // CASCADE is not yet supported for DROP SCHEMA
            // remove all tables at once
            stmt.executeUpdate("DROP TABLE IF EXISTS ${config.schema}._table")
            stmt.executeUpdate("DROP TABLE IF EXISTS ${config.schema}._table2")
            stmt.executeUpdate("DROP SCHEMA IF EXISTS ${config.schema}")
            stmt.executeUpdate("CREATE SCHEMA ${config.schema}")
        }
    }

    override fun resultSetFor(query: String): ResultSet {
        createConnection().use {
            return it.createStatement().executeQuery(query)
        }
    }
}
