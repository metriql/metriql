package com.metriql.warehouse.spi

import java.time.ZoneId

data class WarehouseAuth(val projectId: Int, val userId: Any?, val timezone: ZoneId?, val source: String?)
