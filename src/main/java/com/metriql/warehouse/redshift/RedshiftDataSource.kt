package com.metriql.warehouse.redshift

import com.metriql.warehouse.postgresql.PostgresqlDataSource
import com.metriql.warehouse.postgresql.PostgresqlWarehouse
import com.metriql.warehouse.spi.DbtSettings
import java.util.Properties

class RedshiftDataSource(override val config: PostgresqlWarehouse.PostgresqlConfig) : PostgresqlDataSource(config) {
    override val warehouse = RedshiftWarehouse

    override val dataSourceProperties: Properties by lazy {
        val properties = Properties()
        properties["jdbcUrl"] = "jdbc:redshift://${config.host}:${config.port}/${config.dbname}"
        properties["driverClassName"] = "com.amazon.redshift.jdbc42.Driver"

        properties["dataSource.user"] = config.user
        properties["dataSource.password"] = config.password
        config.connectionParameters?.map { (k, v) ->
            properties[k] = v
        }
        properties
    }

    override fun dbtSettings(): DbtSettings {
        return DbtSettings(
            "redshift",
            mapOf(
                "method" to "database",
                "host" to config.host,
                "port" to config.port,
                "database" to config.dbname,
                "user" to config.user,
                "password" to (config.password ?: ""),
                "schema" to (config.schema ?: "")
            )
        )
    }
}
