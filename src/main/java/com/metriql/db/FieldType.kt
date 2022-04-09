package com.metriql.db

import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.filter.AnyOperatorType
import com.metriql.warehouse.spi.filter.ArrayOperatorType
import com.metriql.warehouse.spi.filter.BooleanOperatorType
import com.metriql.warehouse.spi.filter.DateOperatorType
import com.metriql.warehouse.spi.filter.NumberOperatorType
import com.metriql.warehouse.spi.filter.StringOperatorType
import com.metriql.warehouse.spi.filter.TimeOperatorType
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import kotlin.reflect.KClass

@UppercaseEnum
enum class FieldType(val operatorClass: KClass<out Enum<*>>) {
    STRING(StringOperatorType::class),

    INTEGER(NumberOperatorType::class),
    DECIMAL(NumberOperatorType::class),
    DOUBLE(NumberOperatorType::class),
    LONG(NumberOperatorType::class),

    BOOLEAN(BooleanOperatorType::class),

    DATE(DateOperatorType::class),
    TIME(TimeOperatorType::class),
    TIMESTAMP(TimestampOperatorType::class),
    BINARY(NotImplemented::class),

    ARRAY_STRING(ArrayOperatorType::class),
    ARRAY_INTEGER(AnyOperatorType::class),
    ARRAY_DOUBLE(AnyOperatorType::class),
    ARRAY_LONG(AnyOperatorType::class),
    ARRAY_BOOLEAN(AnyOperatorType::class),
    ARRAY_DATE(AnyOperatorType::class),
    ARRAY_TIME(AnyOperatorType::class),
    ARRAY_TIMESTAMP(AnyOperatorType::class),
    MAP_STRING(NotImplemented::class),

    UNKNOWN(AnyOperatorType::class);

    val isArray: Boolean get() = this.name.startsWith("ARRAY")

    val isMap: Boolean get() = this == MAP_STRING

    val isNumeric: Boolean get() = this == INTEGER || this == DECIMAL || this == DOUBLE || this == LONG

    val arrayElementType: FieldType
        get() {
            if (!isArray) {
                throw IllegalStateException("type is not array")
            }

            return values[ordinal - 10]
        }

    companion object {
        private val values = values()

        val NUMERIC_TYPES = listOf(DOUBLE, INTEGER, LONG)
    }
}

enum class NotImplemented()
