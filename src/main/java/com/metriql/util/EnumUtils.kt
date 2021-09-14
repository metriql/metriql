package com.metriql.util

import com.google.common.base.CaseFormat

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
