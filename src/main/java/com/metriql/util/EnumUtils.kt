package com.metriql.util

import com.google.common.base.CaseFormat
import com.metriql.db.FieldType
import io.netty.handler.codec.http.HttpResponseStatus

val <T : Enum<T>> Enum<T>.serializableName: String
    get() = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name)

val <T : Enum<T>> Enum<T>.toSnakeCase: String
    get() = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_UNDERSCORE, name)

// Careless try, like on swift, returns null if run throws
fun <T> `try?`(run: () -> T): T? {
    return try {
        run()
    } catch (_: Exception) {
        null
    }
}

fun toSnakeCase(value: String): String {
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, value)
}

fun toCamelCase(value: String): String {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, value)
}

fun getOperation(inferredType: FieldType?, operator : String) : Pair<FieldType, Enum<*>> {
    val fieldType = try {
        JsonHelper.convert(operator, FieldType.UNKNOWN.operatorClass.java)
        FieldType.UNKNOWN
    } catch (e: Exception) {
        inferredType ?: throw MetriqlException("Type is required for operator $operator", HttpResponseStatus.BAD_REQUEST)
    }

    val operatorBean: Enum<*> = try {
        JsonHelper.convert(operator, fieldType.operatorClass.java)
    } catch (e: Exception) {
        val values = fieldType.operatorClass.java.enumConstants.joinToString(", ") { toCamelCase(it.name) }
        throw MetriqlException(
            "Invalid operator `${operator}`, available values for type $fieldType is $values",
            HttpResponseStatus.BAD_REQUEST
        )
    }

    return fieldType to operatorBean
}
