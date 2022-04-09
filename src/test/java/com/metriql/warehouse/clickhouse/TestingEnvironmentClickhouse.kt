package com.metriql.warehouse.clickhouse

import com.metriql.tests.TestingServer
import org.testcontainers.containers.ClickHouseContainer
import org.testcontainers.utility.DockerImageName
import java.sql.Connection

object TestingEnvironmentClickhouse : TestingServer<Connection> {

    private val dockerContainer: ClickHouseContainer by lazy(mode = LazyThreadSafetyMode.SYNCHRONIZED) {
        val server = ClickHouseContainer(DockerImageName.parse("yandex/clickhouse-server:21.3.20.1"))
        server.addExposedPort(ClickHouseContainer.HTTP_PORT)
        server.start()
        server
    }

    override val config by lazy {
        ClickhouseWarehouse.ClickhouseConfig(
            "localhost", dockerContainer.getMappedPort(ClickHouseContainer.HTTP_PORT),
            // TODO: dockerContainer.databaseName is not exposed
            "default",
            dockerContainer.username,
            dockerContainer.password,
            ssl = false, usePool = false, connectionParameters = mapOf()
        )
    }

    override val dataSource = ClickhouseDataSource(config)

    override fun getQueryRunner() = dataSource.openConnection()
}
