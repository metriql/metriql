package com.metriql.warehouse.mssqlserver

import com.metriql.warehouse.mysql.BaseMySQLFilters
import com.metriql.warehouse.spi.filter.BooleanOperatorType
import com.metriql.warehouse.spi.filter.WarehouseFilterValue
import com.metriql.warehouse.spi.filter.WarehouseFilters.Companion.validateFilterValue

open class MSSQLFilters : BaseMySQLFilters({ MSSQLMetriqlBridge }) {
    override val booleanOperators: Map<BooleanOperatorType, WarehouseFilterValue> = mapOf(
        BooleanOperatorType.IS to { dimension: String, value: Any?, _ ->
            if (validateFilterValue(value, Boolean::class.java)) {
                "$dimension = 1"
            } else {
                "$dimension = 0"
            }
        }
    )
}
