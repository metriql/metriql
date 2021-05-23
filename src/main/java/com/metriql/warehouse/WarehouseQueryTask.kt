package com.metriql.warehouse

interface WarehouseQueryTask {
    val limit: Int
    val isBackgroundTask: Boolean

    companion object {
        const val MAX_LIMIT = 50000
        const val DEFAULT_LIMIT = 1000
    }
}
