package com.metriql.warehouse.postgresql

import com.metriql.db.FieldType
import com.metriql.report.QueryTask
import com.metriql.util.JdbcUtil
import com.metriql.util.ValidationUtil.stripLiteral
import com.metriql.warehouse.JDBCWarehouse
import com.metriql.warehouse.spi.DbtSettings
import com.metriql.warehouse.spi.SchemaName
import com.metriql.warehouse.spi.Warehouse
import com.metriql.warehouse.spi.WarehouseAuth
import java.sql.Connection
import java.time.ZoneId
import java.time.format.TextStyle
import java.util.Locale
import java.util.Properties

open class PostgresqlDataSource(override val config: PostgresqlWarehouse.PostgresqlConfig, val readOnly: Boolean = true) : JDBCWarehouse(
    config,
    arrayOf("TABLE", "VIEW", "MATERIALIZED VIEW"),
    config.usePool ?: true,
    false,
    config.dbname,
    config.schema
) {
    override val warehouse: Warehouse<*> = PostgresqlWarehouse

    override val dataSourceProperties: Properties by lazy {
        val properties = Properties()
        properties["jdbcUrl"] = "jdbc:postgresql://${config.host}:${config.port}/${config.dbname}"
        properties["driverClassName"] = "org.postgresql.Driver"
        properties["readOnly"] = readOnly

        properties["dataSource.user"] = config.user
        properties["dataSource.password"] = config.password ?: ""
        properties["dataSource.currentSchema"] = config.schema
        properties["dataSource.connectTimeout"] = 10
//        properties["dataSource.socketTimeout"] = 30
        properties["dataSource.loginTimeout"] = 10
        properties["dataSource.ApplicationName"] = "metriql"
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

        val timezone = stripLiteral(timezone.getDisplayName(TextStyle.NARROW, Locale.ENGLISH))
        customProperties["connectionInitSql"] = "SET TIME ZONE '$timezone'"

        return customProperties
    }

    override fun getFieldType(sqlType: Int, dbType: String): FieldType? {
        return JdbcUtil.fromPostgresqlType(dbType)
    }

    override fun getColumnValue(auth: WarehouseAuth, conn: Connection, obj: Any, type: FieldType): Any? {
        return JdbcUtil.fromPostgresqlValue(type, obj)
    }

    override fun createQueryTask(
        auth: WarehouseAuth,
        query: String,
        defaultSchema: String?,
        defaultDatabase: String?,
        limit: Int?,
        isBackgroundTask: Boolean
    ): QueryTask {
        return createSyncQueryTask(
            auth,
            query,
            defaultSchema ?: config.schema,
            defaultDatabase ?: config.dbname,
            limit,
            ignoredErrorCodes = listOf("42", "43", "28", "53", "54", "55", "57") // https://www.postgresql.org/docs/9.6/errcodes-appendix.html
        )
    }

    override fun listSchemaNames(database: String?): List<SchemaName> {
        return super.listSchemaNames(database)
            .filter { !listOf("pg_catalog", "information_schema").contains(it) && !it.startsWith("pg_toast") }
    }

    override fun dbtSettings(): DbtSettings {
        return DbtSettings(
            "postgres",
            mapOf(
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
