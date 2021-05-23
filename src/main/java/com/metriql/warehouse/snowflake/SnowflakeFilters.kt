package com.metriql.warehouse.snowflake

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.filter.BooleanOperatorType
import com.metriql.warehouse.spi.filter.WarehouseFilterValue
import com.metriql.warehouse.spi.filter.WarehouseFilters.Companion.validateFilterValue
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class SnowflakeFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge) {
    override val booleanOperators: Map<BooleanOperatorType, WarehouseFilterValue> = mapOf(
        BooleanOperatorType.IS to { dimension: String, value: Any?, context ->
            "$dimension = ${parseAnyValue(validateFilterValue(value, Boolean::class.java), context)}"
        }
    )

    override fun formatDate(value: String, context: IQueryGeneratorContext): String {
        return if (context.auth.timezone == null) {
            "CAST($value AS DATE)"
        } else {
            "CONVERT_TIMEZONE('${context.auth.timezone}', CAST($value AS DATE))"
        }
    }

    override fun formatTimestamp(value: String, context: IQueryGeneratorContext): String {
        return if (context.auth.timezone == null) {
            "CAST($value AS TIMESTAMP)"
        } else {
            "CONVERT_TIMEZONE('${context.auth.timezone}', CAST($value AS TIMESTAMP))"
        }
    }
}
