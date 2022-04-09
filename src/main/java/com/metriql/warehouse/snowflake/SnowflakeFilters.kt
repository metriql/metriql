package com.metriql.warehouse.snowflake

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class SnowflakeFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge) {
    override fun formatDate(value: String, context: IQueryGeneratorContext): String {
        return if (context.auth.timezone != null) {
            "CONVERT_TIMEZONE('${context.auth.timezone}', CAST($value AS DATE))"
        } else super.formatDate(value, context)
    }

    override fun formatTimestamp(value: String, context: IQueryGeneratorContext): String {
        return if (context.auth.timezone != null) {
            "CONVERT_TIMEZONE('${context.auth.timezone}', CAST($value AS TIMESTAMP))"
        } else super.formatTimestamp(value, context)
    }
}
