package com.metriql.warehouse.spi.filter

import com.metriql.db.FieldType
import com.metriql.util.MetriqlExceptions.SYSTEM_FILTER_TYPE_CANNOT_CAST
import com.metriql.util.RPeriod
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import java.text.NumberFormat
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

typealias WarehouseFilterValue = (dimension: String, value: Any?, context: IQueryGeneratorContext) -> String

/*
+------------------+         +----------------+          +----------------+
|                  |         |                |          |                |
| WarehouseFilters |---------| ANSISQLFilters |----------| AnySqlFilters  |
|                  |         |                |          |                |
+------------------+         +----------------+          +----------------+
If the value parser does not meet warehouses requirements;
An implementor of ANSISQLFilters or AnySqlFilters can override 'parseAnyValue' to output required SQL style of the variable
* */
interface WarehouseFilters {
    val likeEscapeCharacter: Char
    val anyOperators: Map<AnyOperatorType, WarehouseFilterValue>
    val stringOperators: Map<StringOperatorType, WarehouseFilterValue>
    val booleanOperators: Map<BooleanOperatorType, WarehouseFilterValue>
    val numberOperators: Map<NumberOperatorType, WarehouseFilterValue>
    val timestampOperators: Map<TimestampOperatorType, WarehouseFilterValue>
    val timeOperators: Map<TimeOperatorType, WarehouseFilterValue>
    val dateOperators: Map<DateOperatorType, WarehouseFilterValue>
    val arrayOperators: Map<ArrayOperatorType, WarehouseFilterValue>

    /**
     * @param operator: Operator Enum (string, boolean etc.)
     * @param metric: Column identifier, can be an expression, dimension or measure
     * @param value: An optional value which will be used while generating filter
     * @param zoneId: Timezone of the project
     * @return SQL string of generated filter
     * */
    fun generateFilter(context: IQueryGeneratorContext, operator: FilterOperator, metric: String, value: Any?): String {
        val op = when (operator) {
            is AnyOperatorType -> anyOperators[operator]
            is StringOperatorType -> stringOperators[operator]
            is BooleanOperatorType -> booleanOperators[operator]
            is NumberOperatorType -> numberOperators[operator]
            is TimestampOperatorType -> timestampOperators[operator]
            is TimeOperatorType -> timeOperators[operator]
            is DateOperatorType -> dateOperators[operator]
            is ArrayOperatorType -> arrayOperators[operator]
            else -> null
        } ?: throw IllegalStateException("Operator $operator not supported")
        return op.invoke(metric, value, context)
    }

    /**
     * Validated value will than be passed to this function. Converting the value to string that warehouse can understand.
     * @param value: Any value  supported by the warehouse
     * @returns SQL appropriate string format of the value (ex: "TRUE" or "FALSE" for boolean type value)
     * */
    fun parseAnyValue(value: Any?, context: IQueryGeneratorContext, type: FieldType? = null): String

    companion object {
        /**
         * Validates and parses the value. If check fails, function throws an exception.
         * @param T: Type of value
         * @param value: Can be any of the supported types
         * @param clazz: Class of T
         * @returns Variable with type T
         * */
        fun <T> validateFilterValue(value: Any?, expectedClazz: Class<T>): T {
            return when (expectedClazz) {
                List::class.java -> {
                    (value as? List<*>) ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name))
                }
                Number::class.java -> {
                    if (value is String) {
                        try {
                            NumberFormat.getInstance().parse(value)
                        } catch (e: Exception) {
                            null
                        }
                    } else {
                        value as? Number
                    } ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name))
                }
                String::class.java -> (value as? String) ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name))
                Boolean::class.java -> (value as? Boolean) ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name))
                RPeriod::class.java -> RPeriod.fromName(value as? String ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name)))
                Duration::class.java -> Duration.parse(value as? String) ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name))
                LocalDate::class.java -> {
                    val dateTimeFormat = try {
                        LocalDate.parse(value.toString(), DateTimeFormatter.ISO_DATE_TIME)
                    } catch (_: Exception) {
                        null
                    }

                    val dateFormat = try {
                        LocalDate.parse(value.toString(), DateTimeFormatter.ISO_DATE)
                    } catch (_: Exception) {
                        null
                    }

                    dateTimeFormat ?: (dateFormat ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name)))
                }
                Instant::class.java -> Instant.parse(value.toString()) ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name))
                LocalTime::class.java -> LocalTime.parse(value.toString()) ?: throw SYSTEM_FILTER_TYPE_CANNOT_CAST.exceptionFromObject(Pair(value, expectedClazz.name))
                else -> throw IllegalStateException("$expectedClazz parsing is not supported")
            } as T
        }
    }

    fun validateTimestampOperator(value: Any?): Any {
        return try {
            validateFilterValue(value, Instant::class.java)
        } catch (_: Throwable) {
            validateFilterValue(value, LocalDate::class.java)
        }
    }
}
