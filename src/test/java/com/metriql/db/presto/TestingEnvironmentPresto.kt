package com.metriql.db.presto

import com.metriql.HostPortProvider
import com.metriql.tests.TestingServer
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.presto.PrestoWarehouse
import io.trino.jdbc.TrinoConnection
import org.testcontainers.containers.TrinoContainer
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.Properties

object TestingEnvironmentPresto : TestingServer<Unit, TrinoConnection>() {
    private const val PRESTO_HOST = "127.0.0.1"
    private const val PRESTO_CATALOG = "memory"
    private const val PRESTO_USER = "metriql_user"
    private const val PRESTO_PORT = 8080

    private val dockerContainer: TrinoContainer by lazy(mode = LazyThreadSafetyMode.SYNCHRONIZED) {
        val server = TrinoContainer("trinodb/trino")
            .withExposedPorts(PRESTO_PORT)
        server.start()
        server
    }

    private fun getJdbcUrl(hostPortProvider: HostPortProvider): String? {
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

    override fun createConnection(): TrinoConnection {
        val properties = Properties().apply {
            setProperty("SSL", "false")
        }

        val connection = DriverManager.getConnection(getJdbcUrl(dockerContainer::getMappedPort), properties)
        return connection.unwrap(TrinoConnection::class.java)
    }

    @Synchronized
    override fun init() {
        createConnection().use {
            val stmt = it.createStatement()
            // CASCADE is not yet supported for DROP SCHEMA
            // remove all tables at once
            stmt.executeUpdate("DROP TABLE IF EXISTS ${config.schema}.filter_tests")
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
