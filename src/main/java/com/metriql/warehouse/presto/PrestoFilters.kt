package com.metriql.warehouse.presto

import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.ANSISQLFilters

class PrestoFilters(override val bridge: () -> WarehouseMetriqlBridge) : ANSISQLFilters(bridge)
