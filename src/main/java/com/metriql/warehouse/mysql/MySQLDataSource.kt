package com.metriql.warehouse.mysql

import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.warehouse.JDBCWarehouse
import com.metriql.warehouse.spi.SchemaName
import com.metriql.warehouse.spi.WarehouseAuth
import java.time.Instant
import java.time.ZoneId
import java.util.Properties

class MySQLDataSource(override val config: MySQLWarehouse.MysqlConfig) : JDBCWarehouse(
    config,
    arrayOf("TABLE", "VIEW"),
    config.usePool,
    true,
    config.database,
    null
) {
    override val warehouse = MySQLWarehouse

    override val dataSourceProperties by lazy {
        val properties = Properties()
        properties["driverClassName"] = "com.mysql.cj.jdbc.Driver"
        properties["jdbcUrl"] = "jdbc:mysql://${config.host}:${config.port}/${config.database}"

        properties["dataSource.user"] = config.user
        properties["dataSource.password"] = config.password
        config.connectionParameters?.map { (k, v) ->
            properties[k] = v
        }
        properties
    }

    override fun getPropertiesForSession(timezone: ZoneId?): Properties {
        if (timezone == null) {
            return dataSourceProperties
        }

        val customProperties = dataSourceProperties.clone() as Properties

        val offset = timezone.rules.getOffset(Instant.now())
        // workaround for different values such as GMT, Z, UTC
        val zone = if (offset.totalSeconds == 0) "+00:00" else offset
        customProperties["connectionInitSql"] = "SET time_zone = '$zone'"

        return customProperties
    }

    override fun listSchemaNames(database: String?): List<SchemaName> {
        throw IllegalStateException("Schema is not supported for MySQL")
    }

    override fun createQueryTask(
        auth: WarehouseAuth,
        query: QueryResult.QueryStats.QueryInfo,
        defaultSchema: String?,
        defaultDatabase: String?,
        limit: Int?,
        isBackgroundTask: Boolean
    ): QueryTask {
        return return createSyncQueryTask(
            auth,
            query,
            null, // MySQL does not have any schema
            defaultSchema ?: config.database, // Default schema is the database here
            limit
        )
    }
}
