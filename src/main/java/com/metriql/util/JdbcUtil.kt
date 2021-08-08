package com.metriql.util

import com.google.common.io.ByteStreams
import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.QueryColumn
import io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR
import org.postgresql.util.PGobject
import java.io.IOException
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types
import java.time.LocalTime
import java.time.ZoneId
import java.util.ArrayList
import java.util.Calendar
import java.util.TimeZone
import java.util.logging.Logger

object JdbcUtil {
    fun fromGenericJDBCTypeFieldType(jdbcType: Int): FieldType? {
        return when (jdbcType) {
            Types.VARBINARY, Types.BINARY, Types.LONGVARBINARY -> FieldType.BINARY
            Types.BIGINT -> FieldType.LONG
            Types.TINYINT, Types.INTEGER, Types.SMALLINT -> FieldType.INTEGER
            Types.DECIMAL -> FieldType.DOUBLE
            Types.BOOLEAN, Types.BIT -> FieldType.BOOLEAN
            Types.DATE -> FieldType.DATE
            Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> FieldType.TIMESTAMP
            Types.TIME, Types.TIME_WITH_TIMEZONE -> FieldType.TIME
            Types.DOUBLE, Types.FLOAT, Types.NUMERIC, Types.REAL -> FieldType.DOUBLE
            Types.LONGVARCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.CHAR -> FieldType.STRING
            else -> null
        }
    }

    // converts rs.getObject to our Java type. If it returns null, Jackson makes use of the rs.getObject
    fun fromPostgresqlValue(type: FieldType, obj: Any): Any? {
        if (type.isMap) {
            return JsonHelper.read((obj as PGobject).value)
        }

        return null
    }

    fun fromPostgresqlType(name: String): FieldType? {
        if (name.startsWith("_")) {
            if (name.startsWith("_int")) {
                return FieldType.ARRAY_LONG
            }
            if (name == "_bool") {
                return FieldType.ARRAY_BOOLEAN
            }
            if (name == "_text" || name == "_varchar") {
                return FieldType.ARRAY_STRING
            }
            if (name.startsWith("_float")) {
                return FieldType.ARRAY_DOUBLE
            }
        }

        if (name == "citext") {
            return FieldType.STRING
        }
        if (name == "jsonb" || name == "json") {
            return FieldType.MAP_STRING
        }
        if (name == "unknown") {
            return FieldType.STRING
        }
        if (name == "interval") {
            return FieldType.LONG
        }

        return null
    }

    fun getJavaObject(
        type: FieldType?,
        resultSet: ResultSet,
        columnIndex: Int,
        zone: ZoneId?,
        typeMapper: (Int, String) -> FieldType
    ): Any? {
        if (type == null) {
            return resultSet.getObject(columnIndex)
        }
        var obg = when (type) {
            FieldType.STRING -> resultSet.getString(columnIndex)
            FieldType.LONG -> resultSet.getLong(columnIndex)
            FieldType.INTEGER -> resultSet.getInt(columnIndex)
            FieldType.DECIMAL -> {
                val bigDecimal = resultSet.getBigDecimal(columnIndex)
                bigDecimal?.toDouble()
            }
            FieldType.DOUBLE -> resultSet.getDouble(columnIndex)
            FieldType.BOOLEAN -> resultSet.getBoolean(columnIndex)
            FieldType.TIMESTAMP -> {
                if (zone != null) {
                    val calendar = Calendar.getInstance(TimeZone.getTimeZone(zone))
                    resultSet.getTimestamp(columnIndex, calendar)?.toInstant()?.atZone(zone)?.toLocalDateTime()
                } else {
                    resultSet.getTimestamp(columnIndex)?.toInstant()
                }
            }
            FieldType.DATE -> {
                resultSet.getDate(columnIndex)?.toLocalDate()
            }
            FieldType.TIME -> {
                val time = resultSet.getTime(columnIndex)
                if (zone != null && time != null) {
                    // TODO: find an efficient way to remove zone
                    LocalTime.parse(time.toString())
                } else {
                    time?.toLocalTime()
                }
            }
            FieldType.BINARY -> {
                resultSet.getBinaryStream(columnIndex)?.let {
                    try {
                        ByteStreams.toByteArray(it)
                    } catch (e: IOException) {
                        logger.warning("Error while de-serializing BINARY type")
                        null
                    }
                }
            }
            else -> when {
                type.isArray -> {
                    val array = resultSet.getArray(columnIndex)

                    if (array == null) {
                        null
                    } else {
                        var type = try {
                            val columnType = array.baseType
                            fromGenericJDBCTypeFieldType(columnType)
                                ?: typeMapper.invoke(columnType, array.baseTypeName)
                        } catch (e: java.lang.UnsupportedOperationException) {
                            FieldType.UNKNOWN
                        }

                        try {
                            val arrayRs = array.resultSet
                            val list = mutableListOf<Any?>()
                            while (arrayRs.next()) {
                                list.add(getJavaObject(type, arrayRs, 2, zone, typeMapper))
                            }
                        } catch (e: SQLFeatureNotSupportedException) {
                            array.array
                        }
                    }
                }
                type.isMap -> {
                    return resultSet.getObject(columnIndex)
                }
                else -> {
                    throw IllegalStateException("Unknown type $type")
                }
            }
        }

        if (resultSet.wasNull()) {
            obg = null
        }

        return obg
    }

    @Throws(SQLException::class)
    fun toQueryResult(resultSet: ResultSet, typeMapper: (Int, String) -> FieldType, mapper: (FieldType, Any) -> Any?, zone: ZoneId?): QueryResult {
        val result: MutableList<List<Any?>> = ArrayList()
        val metadata: MutableList<QueryColumn> = ArrayList()
        val metaData = resultSet.metaData
        val columnCount = metaData.columnCount
        for (i in 1 until columnCount + 1) {
            var type = try {
                val columnType = metaData.getColumnType(i)
                fromGenericJDBCTypeFieldType(columnType)
                    ?: typeMapper.invoke(columnType, metaData.getColumnTypeName(i))
            } catch (e: java.lang.UnsupportedOperationException) {
                logger.warning("Error while converting sql type to metriql type")
                FieldType.UNKNOWN
            }
            metadata.add(QueryColumn(metaData.getColumnName(i), i - 1, type, metaData.getColumnTypeName(i)))
        }
        while (resultSet.next()) {
            val rowBuilder = mutableListOf(*arrayOfNulls<Any>(columnCount))
            for (i in 0 until columnCount) {
                val (columnName, _, type) = metadata[i]
                val columnIndex = i + 1
                val javaObject = try {
                    getJavaObject(type, resultSet, columnIndex, zone, typeMapper)
                } catch (e: Exception) {
                    throw MetriqlException("Error while fetching column `$columnName [$type]: ${e.message}`", INTERNAL_SERVER_ERROR)
                }

                val value = if (javaObject != null) {
                    mapper(type ?: FieldType.UNKNOWN, javaObject) ?: javaObject
                } else {
                    null
                }

                rowBuilder[i] = value
            }
            result.add(rowBuilder)
        }
        return QueryResult(metadata, result)
    }

    private val logger = Logger.getLogger(this::class.java.name)
}
