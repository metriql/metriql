package com.metriql.warehouse.snowflake

import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.ValidationUtil.stripLiteral
import com.metriql.warehouse.JDBCWarehouse
import com.metriql.warehouse.spi.DbtSettings
import com.metriql.warehouse.spi.WarehouseAuth
import io.netty.handler.codec.http.HttpResponseStatus
import java.sql.Connection
import java.time.ZoneId
import java.time.format.TextStyle
import java.util.Locale
import java.util.Properties

class SnowflakeDataSource(override val config: SnowflakeWarehouse.SnowflakeConfig) : JDBCWarehouse(
    config,
    arrayOf("TABLE", "BASE TABLE", "VIEW"),
    config.usePool ?: true,
    true,
    config.database,
    config.schema
) {
    override val warehouse = SnowflakeWarehouse

    override val dataSourceProperties by lazy {
        val properties = Properties()

        properties["jdbcUrl"] = if (config.regionId != null) {
            "jdbc:snowflake://${config.account}.${config.regionId}.snowflakecomputing.com"
        } else {
            "jdbc:snowflake://${config.account}.snowflakecomputing.com"
        }
        properties["driverClassName"] = "net.snowflake.client.jdbc.SnowflakeDriver"
        properties["connectionInitSql"] = "ALTER SESSION SET JDBC_TREAT_DECIMAL_AS_INT = FALSE, TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_LTZ', QUERY_TAG = 'metriql', WEEK_START = 1"

        properties["dataSource.account"] = config.account
        properties["dataSource.db"] = config.database
        properties["dataSource.user"] = config.user
        when {
            config.password != null -> {
                properties["dataSource.password"] = config.password
            }
            config.private_key_path != null -> {
                properties["dataSource.private_key_path"] = config.private_key_path
                properties["dataSource.private_key_passphrase"] = config.private_key_passphrase
            }
            else -> {
                throw MetriqlException("No authentication method is set for Snowflake datasource, either `password` or `private_key_path` must be set", HttpResponseStatus.BAD_REQUEST)
            }
        }
        properties["dataSource.schema"] = config.schema
        properties["dataSource.loginTimeout"] = "10"
        properties["dataSource.networkTimeout"] = "10"
        properties["dataSource.application"] = "metriql"
        properties["dataSource.role"] = ""
        if (config.warehouse != null) {
            properties["dataSource.warehouse"] = config.warehouse
        }
        if (config.role != null) {
            properties["dataSource.role"] = config.role
        }
        config.connectionParameters?.forEach { (k, v) ->
            properties["dataSource.$k"] = v
        }

        properties
    }

    override fun getPropertiesForSession(timezone: ZoneId?): Properties {
        if (timezone == null) {
            return dataSourceProperties
        }

        val customProperties = dataSourceProperties.clone() as Properties

        val timezone = stripLiteral(timezone.getDisplayName(TextStyle.NARROW, Locale.ENGLISH))
        customProperties["connectionInitSql"] = "${customProperties["connectionInitSql"]}, TIMEZONE = '$timezone'"
        return customProperties
    }

    override fun getColumnValue(auth: WarehouseAuth, conn: Connection, obj: Any, type: FieldType): Any? {
        return when (type) {
            FieldType.MAP_STRING -> {
                // ""{\"a\": 1}"" -> This is how snowflake returns variant from JDBC
                // We do the following operations to drop first and last quotes also escape the string
                var unEscapedMap = obj.toString().replace("\\", "").toCharArray()
                if (unEscapedMap.first() == '"' && unEscapedMap.last() == '"') {
                    unEscapedMap = unEscapedMap.drop(1).dropLast(1).toCharArray()
                }

                JsonHelper.read(unEscapedMap.joinToString(""), ObjectNode::class.java)
            }
            else -> {
                obj
            }
        }
    }

    override fun getFieldType(sqlType: Int, dbType: String): FieldType? {
        return when (dbType.lowercase()) {
            "variant" -> FieldType.MAP_STRING
            else -> null
        }
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
            defaultSchema ?: config.schema,
            defaultDatabase ?: config.database,
            limit = limit
        )
    }

    override fun dbtSettings(): DbtSettings {
        return DbtSettings(
            "snowflake",
            listOfNotNull(
                "account" to if (config.regionId != null) "${config.account}.${config.regionId}" else config.account,
                "user" to config.user,
                "database" to config.database,
                "schema" to config.schema,
                config.warehouse?.let { "warehouse" to it } ?: null,
                config.password?.let { "password" to it } ?: null,
                config.private_key_passphrase?.let { "private_key_passphrase" to it } ?: null,
                config.private_key_path?.let { "private_key_path" to it } ?: null,
                ).toMap()
        )
    }
}
