package com.metriql.warehouse.spi

import java.time.ZoneId

data class WarehouseAuth(val projectId: String, val userId: Any?, val timezone: ZoneId?, val source: String?)
