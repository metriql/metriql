package com.metriql.db.mssqlserver

import com.metriql.DockerContainer
import com.metriql.db.TestingServer
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.mssqlserver.MSSQLDataSource
import com.metriql.warehouse.mssqlserver.MSSQLWarehouse
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

object TestingEnvironmentMSSQLServer : TestingServer<Unit, Connection>() {
    private const val MSSQL_PORT = 1433
    private const val MSSQL_PASSWORD = "sql_server_Really_complex_Pass"
    private const val MSSQL_ROOT_USER = "sa"
    private const val MSSQL_DATABASE = "model"

    private val dockerContainer: DockerContainer by lazy(mode = LazyThreadSafetyMode.SYNCHRONIZED) {
        DockerContainer(
            "mcr.microsoft.com/mssql/server:2017-latest",
            listOf(MSSQL_PORT),
            mapOf(
                "ACCEPT_EULA" to "true",
                "TZ" to "UTC",
                "MSSQL_PID" to "Developer",
                "MSSQL_SA_PASSWORD" to MSSQL_PASSWORD
            )
        ) {
            runQueryAsRoot(it, "SELECT 1")
        }
    }

    override val config = MSSQLWarehouse.MSSQLServerConfig(
        "127.0.0.1",
        dockerContainer.getHostPort(MSSQL_PORT),
        MSSQL_DATABASE,
        "rakam_test",
        MSSQL_ROOT_USER,
        MSSQL_PASSWORD,
        false,
        mapOf()
    )

    val dataSource = MSSQLDataSource(config)

    override fun getTableReference(tableName: String): String {
        return "${ValidationUtil.quoteIdentifier("rakam_test")}.${ValidationUtil.quoteIdentifier(tableName)}"
    }

    override fun createConnection(): Connection {
        return dataSource.openConnection()
    }

    override fun init() {
        createConnection().use {
            val stmt = it.createStatement()
            stmt.execute("DROP TABLE IF EXISTS rakam_test._table2")
            stmt.execute("DROP TABLE IF EXISTS rakam_test._table")
            stmt.execute("DROP TABLE IF EXISTS rakam_test.filter_tests")
            stmt.execute("DROP TABLE IF EXISTS rakam_test.warehouse_test")
            stmt.execute("DROP SCHEMA IF EXISTS rakam_test")
            stmt.execute("CREATE SCHEMA rakam_test")
        }
    }

    private fun getJdbcUrl(hostPortProvider: DockerContainer.HostPortProvider, user: String, password: String): String? {
        return "jdbc:sqlserver://localhost:${hostPortProvider.getHostPort(MSSQL_PORT)};user=$MSSQL_ROOT_USER;password=$password;databaseName=$MSSQL_DATABASE"
    }

    private fun runQueryAsRoot(hostPortProvider: DockerContainer.HostPortProvider, query: String) {
        DriverManager.getConnection(getJdbcUrl(hostPortProvider, MSSQL_ROOT_USER, MSSQL_PASSWORD)).use { conn -> conn.createStatement().use { stmt -> stmt.execute(query) } }
    }

    override fun resultSetFor(query: String): ResultSet {
        val connection = createConnection()
        return connection.createStatement().executeQuery(query)
    }
}
