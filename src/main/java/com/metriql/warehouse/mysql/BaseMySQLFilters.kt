package com.metriql.warehouse.mysql

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.filter.ArrayOperatorType
import com.metriql.warehouse.spi.filter.WarehouseFilterValue
import com.metriql.warehouse.spi.filter.WarehouseFilters.Companion.validateFilterValue
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

open class BaseMySQLFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge) {

    override fun formatTimestamp(value: String, context: IQueryGeneratorContext): String {
        return "CAST($value AS DATETIME)"
    }

    override val arrayOperators: Map<ArrayOperatorType, WarehouseFilterValue> = mapOf(
        ArrayOperatorType.INCLUDES to { dimension: String, value: Any?, _ ->
            val validatedValue = validateFilterValue(value, List::class.java)
            "FIND_IN_SET($dimension, '${validatedValue.joinToString(",") { it.toString() }}') > 1"
        },
        ArrayOperatorType.NOT_INCLUDES to { dimension: String, value: Any?, _ ->
            val validatedValue = validateFilterValue(value, List::class.java)
            "FIND_IN_SET($dimension, '${validatedValue.joinToString(",") { it.toString() }}') == 0"
        }
    )
}
