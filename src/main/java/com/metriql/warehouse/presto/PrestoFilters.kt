package com.metriql.warehouse.presto

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.filter.BooleanOperatorType
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import com.metriql.warehouse.spi.filter.WarehouseFilterValue
import com.metriql.warehouse.spi.filter.WarehouseFilters.Companion.validateFilterValue
import java.time.Instant
import java.time.LocalDate

class PrestoFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge) {

    override val booleanOperators: Map<BooleanOperatorType, WarehouseFilterValue> = mapOf(
        BooleanOperatorType.IS to { dimension: String, value: Any?, context ->
            "$dimension = ${parseAnyValue(validateFilterValue(value, Boolean::class.java), context)}"
        }
    )

    override val timestampOperators: Map<TimestampOperatorType, WarehouseFilterValue> = mapOf(
        TimestampOperatorType.EQUALS to { dimension: String, value: Any?, context ->
            "$dimension = from_iso8601_timestamp(${parseAnyValue(validateTimestampOperator(value), context)})"
        },
        TimestampOperatorType.GREATER_THAN to { dimension: String, value: Any?, context ->
            "$dimension > from_iso8601_timestamp(${parseAnyValue(validateTimestampOperator(value), context)})"
        },
        TimestampOperatorType.LESS_THAN to { dimension: String, value: Any?, context ->
            "$dimension < from_iso8601_timestamp(${parseAnyValue(validateTimestampOperator(value), context)})"
        }
    )

    private fun validateTimestampOperator(value: Any?): Any {
        return try {
            validateFilterValue(value, Instant::class.java)
        } catch (_: Throwable) {
            validateFilterValue(value, LocalDate::class.java)
        }
    }
}
