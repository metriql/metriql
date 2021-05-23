package com.metriql.warehouse.postgresql

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.filter.ArrayOperatorType
import com.metriql.warehouse.spi.filter.StringOperatorType
import com.metriql.warehouse.spi.filter.WarehouseFilterValue
import com.metriql.warehouse.spi.filter.WarehouseFilters.Companion.validateFilterValue

open class PostgresqlFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge) {
    override val arrayOperators: Map<ArrayOperatorType, WarehouseFilterValue> = mapOf(
        ArrayOperatorType.INCLUDES to { dimension: String, value: Any?, context ->
            val validatedValue = validateFilterValue(value, List::class.java)
            "$dimension @> ARRAY[${parseAnyValue(validatedValue, context)}]"
        },
        ArrayOperatorType.NOT_INCLUDES to { dimension: String, value: Any?, context ->
            val validatedValue = validateFilterValue(value, List::class.java)
            "NOT ($dimension @> ARRAY[${parseAnyValue(validatedValue, context)}])"
        }
    )

    override val stringOperators: Map<StringOperatorType, WarehouseFilterValue> = super.stringOperators.plus(
        StringOperatorType.REGEX to { dimension: String, value: Any?, context ->
            "$dimension ~* ${parseAnyValue(validateFilterValue(value, String::class.java), context)}"
        }
    )
}
