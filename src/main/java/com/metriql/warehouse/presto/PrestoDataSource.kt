package com.metriql.warehouse.presto

import com.metriql.db.FieldType
import com.metriql.report.QueryTask
import com.metriql.warehouse.JDBCWarehouse
import com.metriql.warehouse.spi.WarehouseAuth
import io.trino.jdbc.TrinoConnection
import java.util.Properties

class PrestoDataSource(override val config: PrestoWarehouse.PrestoConfig) : JDBCWarehouse(
    config,
    arrayOf("BASE TABLE", "TABLE", "VIEW"),
    config.usePool ?: true,
    true,
    config.catalog,
    config.schema
) {
    override val warehouse = PrestoWarehouse

    override val dataSourceProperties: Properties by lazy {
        val properties = Properties()
        properties["jdbcUrl"] = "jdbc:trino://${config.host}:${config.port}/${config.catalog}/${config.schema}"
        properties["driverClassName"] = "io.trino.jdbc.TrinoDriver"

        properties["dataSource.user"] = config.user
        properties["dataSource.password"] = config.password
        config.connectionParameters?.map { (k, v) ->
            properties[k] = v
        }
        properties
    }

    override fun getFieldType(sqlType: Int, dbType: String): FieldType? {
        /*
        val baseType = rawType.base.toLowerCase(Locale.ENGLISH)
        if (baseType == "array") {
            return toFieldType(rawType.parameters[0].typeSignature)?.convertToArrayType()
        }
        if (baseType == "map") {
            val keyType =
                toFieldType(rawType.parameters[0].typeSignature)
            if (keyType == FieldType.STRING) {
                val valueType =
                    toFieldType(rawType.parameters[1].typeSignature)
                return valueType?.convertToMapValueType()
            }
        }
        return when (baseType) {
            "boolean" -> FieldType.BOOLEAN
            "bigint" -> FieldType.LONG
            "integer" -> FieldType.INTEGER
            "smallint" -> FieldType.INTEGER
            "tinyint" -> FieldType.INTEGER
            "real" -> FieldType.DOUBLE
            "double" -> FieldType.DOUBLE
            "varchar" -> FieldType.STRING
            "char" -> FieldType.STRING
            "varbinary" -> FieldType.STRING
            "time" -> FieldType.TIME
            "time with time zone" -> FieldType.TIME
            "timestamp" -> FieldType.TIMESTAMP
            "timestamp with time zone" -> FieldType.TIMESTAMP
            "date" -> FieldType.DATE
            "decimal" -> FieldType.DECIMAL
            "row" -> FieldType.ROW
            else -> throw IllegalArgumentException("Unknown PRESTO type $rawType")
        }
        * */
        return null
    }

    override fun createQueryTask(
        auth: WarehouseAuth,
        query: String,
        defaultSchema: String?,
        defaultDatabase: String?,
        limit: Int?,
        isBackgroundTask: Boolean
    ): QueryTask {
        val connection = openConnection(auth.timezone)
        // Set timezone here
        if (connection is TrinoConnection) {
            auth.timezone?.let { timeZone -> connection.timeZoneId = timeZone.id }
        }

        return createSyncQueryTask(
            auth,
            query,
            defaultDatabase ?: config.catalog,
            defaultSchema ?: config.schema,
            limit
        )
    }
}
