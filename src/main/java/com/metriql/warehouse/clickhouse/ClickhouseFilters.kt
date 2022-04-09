package com.metriql.warehouse.clickhouse

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class ClickhouseFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge) {
    override fun formatTimestamp(value: String, context: IQueryGeneratorContext): String {
        return if (context.auth.timezone != null) {
            "parseDateTimeBestEffort($value, '${context.auth.timezone}')"
        } else {
            "parseDateTimeBestEffort($value)"
        }
    }
}
