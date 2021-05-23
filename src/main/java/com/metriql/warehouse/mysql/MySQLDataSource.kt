package com.metriql.warehouse.mysql

import com.metriql.report.QueryTask
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.JDBCWarehouse
import com.metriql.warehouse.spi.SchemaName
import com.metriql.warehouse.spi.WarehouseAuth
import java.time.ZoneId
import java.time.format.TextStyle
import java.util.Locale
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
        properties["driverClassName"] = "com.mysql.jdbc.Driver"
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

        val timezone = ValidationUtil.checkLiteral(timezone.getDisplayName(TextStyle.NARROW, Locale.ENGLISH))
        customProperties["connectionInitSql"] = "SET time_zone = '$timezone'"

        return customProperties
    }

    override fun listSchemaNames(database: String?): List<SchemaName> {
        throw IllegalStateException("Schema is not supported for MySQL")
    }

    override fun createQueryTask(
        auth: WarehouseAuth,
        query: String,
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
