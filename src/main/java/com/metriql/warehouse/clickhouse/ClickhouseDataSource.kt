package com.metriql.warehouse.clickhouse

import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.warehouse.JDBCWarehouse
import com.metriql.warehouse.spi.DbtSettings
import com.metriql.warehouse.spi.WarehouseAuth
import io.trino.jdbc.TrinoConnection
import java.util.Properties

class ClickhouseDataSource(override val config: ClickhouseWarehouse.ClickhouseConfig) : JDBCWarehouse(
    config,
    arrayOf("TABLE", "VIEW"),
    config.usePool,
    true,
    config.database,
    null
) {
    override val warehouse = ClickhouseWarehouse

    override val dataSourceProperties: Properties by lazy {
        val properties = Properties()
        properties["jdbcUrl"] = "jdbc:clickhouse://${config.host}:${config.port}/${config.database}"
        properties["driverClassName"] = "ru.yandex.clickhouse.ClickHouseDriver"

        properties["dataSource.user"] = config.user
        properties["dataSource.password"] = config.password
        config.connectionParameters?.map { (k, v) ->
            properties[k] = v
        }
        properties
    }

    override fun dbtSettings(): DbtSettings {
        return DbtSettings(
            "clickhouse",
            mapOf(
                "host" to config.host,
                "port" to config.port,
                "schema" to config.database,
                "user" to config.user,
                "password" to config.password
            )
        )
    }

    override fun createQueryTask(
        auth: WarehouseAuth,
        query: QueryResult.QueryStats.QueryInfo,
        defaultSchema: String?,
        defaultDatabase: String?,
        limit: Int?,
        isBackgroundTask: Boolean
    ): QueryTask {
        return createSyncQueryTask(
            auth,
            query,
            null,
            defaultDatabase ?: config.database,
            limit
        )
    }
}
