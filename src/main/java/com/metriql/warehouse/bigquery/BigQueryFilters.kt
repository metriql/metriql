package com.metriql.warehouse.bigquery

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.filter.StringOperatorType
import com.metriql.warehouse.spi.filter.WarehouseFilterValue
import com.metriql.warehouse.spi.filter.WarehouseFilters.Companion.validateFilterValue
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class BigQueryFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge) {
    override val stringOperators: Map<StringOperatorType, WarehouseFilterValue> = super.stringOperators.plus(
        StringOperatorType.REGEX to { dimension: String, value: Any?, context ->
            "REGEXP_CONTAINS($dimension, r${parseAnyValue(validateFilterValue(value, String::class.java), context)})"
        }
    )

    override fun formatTimestamp(value: String, context: IQueryGeneratorContext): String {
        return "TIMESTAMP($value, '${context.auth.timezone ?: "UTC"}')"
    }

    override fun formatDate(value: String, context: IQueryGeneratorContext): String {
        return "DATE($value, '${context.auth.timezone ?: "UTC"}')"
    }
}
