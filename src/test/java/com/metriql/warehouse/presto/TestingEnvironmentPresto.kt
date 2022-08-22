package com.metriql.warehouse.presto

import com.metriql.tests.TestingServer
import org.testcontainers.containers.TrinoContainer
import java.sql.Connection

object TestingEnvironmentPresto : TestingServer<Connection> {
    private const val PRESTO_CATALOG = "memory"
    private const val PRESTO_USER = "metriql_user"
    private const val PRESTO_PORT = 8080

    private val dockerContainer: TrinoContainer by lazy(mode = LazyThreadSafetyMode.SYNCHRONIZED) {
        val server = TrinoContainer("trinodb/trino")
            .withExposedPorts(PRESTO_PORT)
        server.start()
        server
    }

    override val config by lazy {
        PrestoWarehouse.PrestoConfig(
            dockerContainer.host,
            dockerContainer.getMappedPort(PRESTO_PORT),
            PRESTO_CATALOG,
            "rakam_test",
            PRESTO_USER,
            ""
        )
    }

    override val dataSource = PrestoDataSource(config)

    override fun getQueryRunner() = dataSource.openConnection()

    @Synchronized
    override fun init() {
        getQueryRunner().use {
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
}
