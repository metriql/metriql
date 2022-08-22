package com.metriql.warehouse.azure

import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.JDBCDataSource
import com.metriql.warehouse.mssqlserver.MSSQLWarehouse
import java.util.Properties

class AzureSQLDataSource(
    override val config: MSSQLWarehouse.MSSQLServerConfig
) : JDBCDataSource(
    config,
    arrayOf("TABLE", "VIEW"),
    config.usePool,
    false,
    config.database,
    config.schema
) {
    override val warehouse = AzureWarehouse

    override val dataSourceProperties by lazy {
        val properties = Properties()
        properties["jdbcUrl"] = "jdbc:sqlserver://${config.host}:${config.port}"
        properties["driverClassName"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

        properties["dataSource.databaseName"] = config.database
        properties["dataSource.user"] = config.user
        properties["dataSource.password"] = config.password
        properties["dataSource.encrypt"] = "true"
        properties["dataSource.hostNameInCertificate"] = "*.database.windows.net"
        properties["dataSource.loginTimeout"] = "30"
        config.connectionParameters?.map { (k, v) ->
            properties["dataSource.$k"] = v
        }
        properties
    }

    override fun getFieldType(sqlType: Int, dbType: String): FieldType? {
        return when (sqlType) {
            microsoft.sql.Types.DATETIME, microsoft.sql.Types.DATETIMEOFFSET, microsoft.sql.Types.SMALLDATETIME -> FieldType.TIMESTAMP
            else -> FieldType.UNKNOWN
        }
    }

    override fun createQueryTask(
        auth: ProjectAuth,
        query: QueryResult.QueryStats.QueryInfo,
        defaultSchema: String?,
        defaultDatabase: String?,
        limit: Int?,
        isBackgroundTask: Boolean
    ): QueryTask {
        return createSyncQueryTask(
            auth,
            query,
            defaultSchema ?: config.schema,
            defaultDatabase ?: config.database,
            limit
        )
    }
}
