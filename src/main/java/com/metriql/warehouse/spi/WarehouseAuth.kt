package com.metriql.warehouse.spi

import java.time.ZoneId

data class WarehouseAuth(val projectId: Int, val userId: Int?, val timezone: ZoneId?)
