package com.metriql.warehouse.clickhouse

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters

class ClickhouseFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge)
