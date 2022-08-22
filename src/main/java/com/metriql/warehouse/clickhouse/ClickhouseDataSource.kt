package com.metriql.warehouse.clickhouse

import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.service.auth.ProjectAuth
import com.metriql.util.JdbcUtil.fromGenericJDBCTypeFieldType
import com.metriql.warehouse.JDBCDataSource
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.DbtSettings
import ru.yandex.clickhouse.response.ClickHouseColumnInfo
import java.sql.Statement
import java.sql.Types
import java.util.Properties

class ClickhouseDataSource(override val config: ClickhouseWarehouse.ClickhouseConfig) : JDBCDataSource(
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
        val port = if (config.port === 9000) 8123 else config.port
        properties["jdbcUrl"] = "jdbc:clickhouse://${config.host}:$port/${config.database}"
        properties["driverClassName"] = "com.clickhouse.jdbc.ClickHouseDriver"

        properties["dataSource.user"] = config.user
        config.ssl?.let { properties["dataSource.ssl"] = it }
        properties["dataSource.password"] = config.password
        config.connectionParameters?.map { (k, v) ->
            properties["dataSource.$k"] = v
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
            null,
            defaultDatabase ?: config.database,
            limit
        )
    }

    override fun getFieldType(sqlType: Int, dbType: String): FieldType? {
        return when (sqlType) {
            Types.ARRAY -> {
                val arrayType = ClickHouseColumnInfo.parse(dbType, "dummy", null)
                when (fromGenericJDBCTypeFieldType(arrayType.arrayBaseType.jdbcType.vendorTypeNumber)) {
                    FieldType.STRING -> FieldType.ARRAY_STRING
                    FieldType.INTEGER -> FieldType.ARRAY_INTEGER
                    FieldType.DOUBLE -> FieldType.ARRAY_DOUBLE
                    FieldType.LONG -> FieldType.ARRAY_LONG
                    FieldType.BOOLEAN -> FieldType.ARRAY_BOOLEAN
                    FieldType.DATE -> FieldType.ARRAY_DATE
                    FieldType.TIME -> FieldType.ARRAY_TIME
                    FieldType.TIMESTAMP -> FieldType.ARRAY_TIMESTAMP
                    else -> null
                }
            }
            else -> null
        }
    }

    override fun setupConnection(auth: ProjectAuth, statement: Statement, defaultDatabase: String?, defaultSchema: String?, limit: Int?) {
        val conn = statement.connection

        // clickhouse only has schema, not catalog
        if (conn.schema != defaultDatabase) {
            conn.schema = defaultDatabase
        }

        if (statement.maxRows != limit) {
            statement.maxRows = limit ?: WarehouseQueryTask.DEFAULT_LIMIT
        }
    }
}
