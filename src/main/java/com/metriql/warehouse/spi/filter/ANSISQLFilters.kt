package com.metriql.warehouse.spi.filter

import com.metriql.db.FieldType
import com.metriql.util.DefaultJinja
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.RPeriod
import com.metriql.util.ValidationUtil
import com.metriql.util.`try?`
import com.metriql.util.serializableName
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.bridge.getRequiredFunction
import com.metriql.warehouse.spi.filter.WarehouseFilters.Companion.validateFilterValue
import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.getRequiredPostOperation
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

open class ANSISQLFilters(open val bridge: () -> WarehouseMetriqlBridge) : WarehouseFilters {

    override val likeEscapeCharacter: Char = '#'

    /**
     * Convert Java value to ANSI SQL value
     */
    override fun parseAnyValue(value: Any?, context: IQueryGeneratorContext, type: FieldType?): String {
        if (value == null) {
            return "NULL"
        }
        val rawValue = when (value) {
            is String, is LocalTime, is Instant -> "'${ValidationUtil.stripLiteral(value.toString())}'"
            is LocalDate -> "'${ValidationUtil.stripLiteral(value.format(DateTimeFormatter.ISO_DATE))}'"
            is Boolean -> if (value) "TRUE" else "FALSE"
            is Number -> value.toString()
            is List<*> -> "(${value.joinToString(", ") { if (it == null) "NULL" else "'${ValidationUtil.stripLiteral(it.toString())}'" }})"
            else -> "NULL"
        }

        return parseRawValue(rawValue, type, context)
    }

    private fun parseRawValue(rawValue: String, type: FieldType?, context: IQueryGeneratorContext): String {
        return when (type) {
            FieldType.TIMESTAMP -> formatTimestamp(rawValue, context)
            FieldType.DATE -> formatDate(rawValue, context)
            FieldType.TIME -> formatTime(rawValue, context)
            else -> rawValue
        }
    }

    open fun formatTimestamp(value: String, context: IQueryGeneratorContext) = "CAST($value AS TIMESTAMP)"
    open fun formatDate(value: String, context: IQueryGeneratorContext) = "CAST($value AS DATE)"
    open fun formatTime(value: String, context: IQueryGeneratorContext) = "CAST($value AS TIME)"

    override val anyOperators: Map<AnyOperatorType, WarehouseFilterValue> = mapOf(
        AnyOperatorType.IS_SET to { dimensionValue: String, _, _ ->
            "$dimensionValue IS NOT NULL"
        },
        AnyOperatorType.IS_NOT_SET to { dimensionValue: String, _, _ ->
            "$dimensionValue IS NULL"
        }
    )

    override val timestampOperators: Map<TimestampOperatorType, WarehouseFilterValue> = mapOf(
        TimestampOperatorType.EQUALS to { dimension: String, value: Any?, context: IQueryGeneratorContext ->
            timestampFilterGenerator(TimestampOperatorType.EQUALS, dimension, value, context)
        },
        TimestampOperatorType.GREATER_THAN to { dimension: String, value: Any?, context: IQueryGeneratorContext ->
            timestampFilterGenerator(TimestampOperatorType.GREATER_THAN, dimension, value, context)
        },
        TimestampOperatorType.GREATER_THAN_OR_EQUAL to { dimension: String, value: Any?, context: IQueryGeneratorContext ->
            timestampFilterGenerator(TimestampOperatorType.GREATER_THAN_OR_EQUAL, dimension, value, context)
        },
        TimestampOperatorType.LESS_THAN to { dimension: String, value: Any?, context: IQueryGeneratorContext ->
            timestampFilterGenerator(TimestampOperatorType.LESS_THAN, dimension, value, context)
        },
        TimestampOperatorType.LESS_THAN_OR_EQUAL to { dimension: String, value: Any?, context: IQueryGeneratorContext ->
            timestampFilterGenerator(TimestampOperatorType.LESS_THAN_OR_EQUAL, dimension, value, context)
        },
        TimestampOperatorType.BETWEEN to { dimension: String, value: Any?, context: IQueryGeneratorContext ->
            timestampFilterGenerator(TimestampOperatorType.BETWEEN, dimension, value, context)
        }
    )

    override val arrayOperators: Map<ArrayOperatorType, WarehouseFilterValue> = mapOf()

    override val stringOperators: Map<StringOperatorType, WarehouseFilterValue> = mapOf(
        StringOperatorType.EQUALS to { dimension: String, value: Any?, context ->
            "$dimension = ${parseAnyValue(validateFilterValue(value, String::class.java), context)}"
        },
        StringOperatorType.NOT_EQUALS to { dimension: String, value: Any?, context ->
            "$dimension != ${parseAnyValue(validateFilterValue(value, String::class.java), context)}"
        },
        StringOperatorType.IN to { dimension: String, value: Any?, context ->
            val inValues = validateFilterValue(value, List::class.java)
            if (inValues.isEmpty()) {
                "TRUE"
            } else {
                "$dimension IN ${parseAnyValue(inValues, context)}"
            }
        },
        StringOperatorType.NOT_IN to { dimension: String, value: Any?, context ->
            val inValues = validateFilterValue(value, List::class.java)
            if (inValues.isEmpty()) {
                "FALSE"
            } else {
                "$dimension NOT IN ${parseAnyValue(inValues, context)}"
            }
        },
        StringOperatorType.CONTAINS to { dimension: String, value: Any?, context ->
            val validatedValue = "%${validateFilterValue(value, String::class.java)}%"
            "$dimension LIKE ${parseAnyValue(validatedValue, context)}"
        },
        StringOperatorType.NOT_CONTAINS to { dimension: String, value: Any?, context ->
            val validatedValue = "%${validateFilterValue(value, String::class.java)}%"
            "$dimension NOT LIKE ${parseAnyValue(validatedValue, context)}"
        },
        StringOperatorType.STARTS_WITH to { dimension: String, value: Any?, context ->
            val validatedValue = "${validateFilterValue(value, String::class.java)}%"
            "$dimension LIKE ${parseAnyValue(validatedValue, context)}"
        },
        StringOperatorType.ENDS_WITH to { dimension: String, value: Any?, context ->
            val validatedValue = "%${validateFilterValue(value, String::class.java)}"
            "$dimension LIKE ${parseAnyValue(validatedValue, context)}"
        }
    )
    override val booleanOperators: Map<BooleanOperatorType, WarehouseFilterValue> = mapOf(
        BooleanOperatorType.EQUALS to { dimension: String, value: Any?, context ->
            "$dimension = ${parseAnyValue(validateFilterValue(value, Boolean::class.java), context)}"
        },
        BooleanOperatorType.NOT_EQUALS to { dimension: String, value: Any?, context ->
            "$dimension != ${parseAnyValue(validateFilterValue(value, Boolean::class.java), context)}"
        }
    )
    override val numberOperators: Map<NumberOperatorType, WarehouseFilterValue> = mapOf(
        NumberOperatorType.NOT_EQUALS to { dimension: String, value: Any?, context ->
            "$dimension != ${parseAnyValue(validateFilterValue(value, Number::class.java), context)}"
        },
        NumberOperatorType.EQUALS to { dimension: String, value: Any?, context ->
            "$dimension = ${parseAnyValue(validateFilterValue(value, Number::class.java), context)}"
        },
        NumberOperatorType.GREATER_THAN to { dimension: String, value: Any?, context ->
            "$dimension > ${parseAnyValue(validateFilterValue(value, Number::class.java), context)}"
        },
        NumberOperatorType.LESS_THAN to { dimension: String, value: Any?, context ->
            "$dimension < ${parseAnyValue(validateFilterValue(value, Number::class.java), context)}"
        },
        NumberOperatorType.LESS_THAN_OR_EQUAL to { dimension: String, value: Any?, context ->
            "$dimension <= ${parseAnyValue(validateFilterValue(value, Number::class.java), context)}"
        },
        NumberOperatorType.GREATER_THAN_OR_EQUAL to { dimension: String, value: Any?, context ->
            "$dimension >= ${parseAnyValue(validateFilterValue(value, Number::class.java), context)}"
        }
    )
    override val timeOperators: Map<TimeOperatorType, WarehouseFilterValue> = mapOf(
        TimeOperatorType.EQUALS to { dimension: String, value: Any?, context ->
            "$dimension = ${parseAnyValue(validateFilterValue(value, LocalTime::class.java), context)}"
        },
        TimeOperatorType.GREATER_THAN to { dimension: String, value: Any?, context ->
            "$dimension > ${parseAnyValue(validateFilterValue(value, LocalTime::class.java), context)}"
        },
        TimeOperatorType.LESS_THAN_OR_EQUAL to { dimension: String, value: Any?, context ->
            "$dimension <= ${parseAnyValue(validateFilterValue(value, LocalTime::class.java), context)}"
        },
        TimeOperatorType.GREATER_THAN_OR_EQUAL to { dimension: String, value: Any?, context ->
            "$dimension >= ${parseAnyValue(validateFilterValue(value, LocalTime::class.java), context)}"
        },
        TimeOperatorType.LESS_THAN to { dimension: String, value: Any?, context ->
            "$dimension < ${parseAnyValue(validateFilterValue(value, LocalTime::class.java), context)}"
        }
    )
    override val dateOperators: Map<DateOperatorType, WarehouseFilterValue> = mapOf(
        DateOperatorType.EQUALS to { dimension: String, value: Any?, context ->
            dateFilterGenerator(DateOperatorType.EQUALS, dimension, value, context)
        },
        DateOperatorType.GREATER_THAN to { dimension: String, value: Any?, context ->
            dateFilterGenerator(DateOperatorType.GREATER_THAN, dimension, value, context)
        },
        DateOperatorType.LESS_THAN to { dimension: String, value: Any?, context ->
            dateFilterGenerator(DateOperatorType.LESS_THAN, dimension, value, context)
        },
        DateOperatorType.LESS_THAN_OR_EQUAL to { dimension: String, value: Any?, context ->
            dateFilterGenerator(DateOperatorType.LESS_THAN_OR_EQUAL, dimension, value, context)
        },
        DateOperatorType.GREATER_THAN_OR_EQUAL to { dimension: String, value: Any?, context ->
            dateFilterGenerator(DateOperatorType.GREATER_THAN_OR_EQUAL, dimension, value, context)
        },
        DateOperatorType.BETWEEN to { dimension: String, value: Any?, context ->
            dateFilterGenerator(DateOperatorType.BETWEEN, dimension, value, context)
        }
    )

    private fun dateFilterGenerator(type: DateOperatorType, dimension: String, value: Any?, context: IQueryGeneratorContext): String {
        val operator = when (type) {
            DateOperatorType.GREATER_THAN -> ">"
            DateOperatorType.GREATER_THAN_OR_EQUAL -> ">="
            DateOperatorType.LESS_THAN -> "<"
            DateOperatorType.LESS_THAN_OR_EQUAL -> "<="
            DateOperatorType.EQUALS -> "="
            DateOperatorType.BETWEEN -> "BETWEEN"
        }

        return timestampDateFilterGenerator(
            operator,
            { period ->
                val invoke = bridge.invoke()
                val dateAdd = getRequiredPostOperation(invoke.functions, RFunction.DATE_ADD)
                val postOperation = (
                    period.type.postOperation.find { it is DatePostOperation }
                        ?: throw MetriqlException("Post operation is not supported for type", HttpResponseStatus.NOT_FOUND)
                    ) as DatePostOperation
                val now = getRequiredFunction(invoke.functions, RFunction.NOW)
                val value = getRequiredPostOperation(invoke.timeframes.datePostOperations, postOperation).format(now)
                Triple(dateAdd, postOperation.name, value)
            },
            FieldType.DATE,
            DateOperatorType.BETWEEN == type, dimension, value, context
        )
    }

    private fun timestampFilterGenerator(type: TimestampOperatorType, dimension: String, value: Any?, context: IQueryGeneratorContext): String {
        val operator = when (type) {
            TimestampOperatorType.GREATER_THAN -> ">"
            TimestampOperatorType.GREATER_THAN_OR_EQUAL -> ">="
            TimestampOperatorType.LESS_THAN -> "<"
            TimestampOperatorType.LESS_THAN_OR_EQUAL -> "<="
            TimestampOperatorType.EQUALS -> "="
            TimestampOperatorType.BETWEEN -> ">="
        }

        return timestampDateFilterGenerator(
            operator,
            { period ->
                val invoke = bridge.invoke()
                val dateAdd = getRequiredPostOperation(invoke.functions, RFunction.DATE_ADD)
                val postOperation = (
                    period.type.postOperation.find { it is TimestampPostOperation }
                        ?: throw MetriqlException("Post operation is not supported for type", HttpResponseStatus.NOT_FOUND)
                    ) as TimestampPostOperation

                val now = getRequiredFunction(invoke.functions, RFunction.NOW)
                val value = getRequiredPostOperation(invoke.timeframes.timestampPostOperations, postOperation).format(now)
                Triple(dateAdd, postOperation.name, value)
            },
            FieldType.TIMESTAMP,
            TimestampOperatorType.BETWEEN == type, dimension, value, context
        )
    }

    private fun timestampDateFilterGenerator(
        operator: String,
        relativeTimeFormatter: (RPeriod) -> Triple<String, String, String>,
        type: FieldType,
        isRange: Boolean,
        dimension: String,
        value: Any?,
        context: IQueryGeneratorContext,
    ): String {
        val (startDate, endDate) = when (value) {
            is String -> {
                val periodValue = `try?` { validateFilterValue(value, RPeriod::class.java) }
                if (periodValue != null) {
                    val (dateAdd, postOperation, formattedValue) = relativeTimeFormatter(periodValue)
                    val dateStart = if (periodValue.value != 0) DefaultJinja.renderFunction(dateAdd, listOf(formattedValue, postOperation, -periodValue.value)) else formattedValue
                    val finalDateStartValue = parseRawValue(dateStart, type, context)
                    if (isRange) {
                        val start = relativeTimeFormatter(RPeriod.fromName("P0D"))
                        val dateEnd = DefaultJinja.renderFunction(dateAdd, listOf(start.third, RPeriod.Type.DAY, 1))
                        finalDateStartValue to parseRawValue(dateEnd, type, context)
                    } else {
                        finalDateStartValue to finalDateStartValue
                    }
                } else {
                    val singleValue = parseAnyValue(value, context, type)
                    singleValue to singleValue
                }
            }
            is DateRange -> {
                parseAnyValue(value.start, context, type) to parseAnyValue(value.end, context, type)
            }
            is Map<*, *> -> {
                val dateRange = JsonHelper.convert(value, DateRange::class.java)
                parseAnyValue(dateRange.start, context, type) to parseAnyValue(dateRange.end, context, type)
            }
            null -> return "TRUE"
            else -> throw java.lang.IllegalStateException("Only 'String' and 'Map' are accepted values for date filter type")
        }

        return if (isRange) {
            "$dimension >= $startDate AND $dimension < $endDate"
        } else {
            "$dimension $operator $startDate"
        }
    }

    companion object {
        fun convertTimestampFilterToDates(type: TimestampOperatorType, value: Any?): DateRange? {
            return when (value) {
                is String -> {
                    val periodValue = `try?` { validateFilterValue(value, RPeriod::class.java) }
                    if (periodValue != null) {
                        val dateStart = LocalDate.now().minus(periodValue.value.toLong(), periodValue.type.unit).plusDays(1)
                        val dateEnd = LocalDate.now().plusDays(1)
                        if (type == TimestampOperatorType.BETWEEN) {
                            DateRange(dateStart, dateEnd)
                        } else {
                            DateRange(dateStart, dateStart)
                        }
                    } else {
                        val validatedValue = validateFilterValue(value, LocalDate::class.java)
                        DateRange(validatedValue, validatedValue)
                    }
                }
                is DateRange -> value
                is Map<*, *> -> {
                    val dateRange = JsonHelper.convert(value, DateRange::class.java)
                    DateRange(dateRange.start, dateRange.end.plusDays(1))
                }
                null -> null
                else -> throw java.lang.IllegalStateException("Only 'String' and 'Map' are accepted values for ${type.serializableName}")
            }
        }
    }
}
