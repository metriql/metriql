package com.metriql.warehouse.presto

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.WarehousePostOperation
import com.metriql.warehouse.spi.function.WarehouseTimeframes

class PrestoTimeframes : WarehouseTimeframes {
    // TODO
    override val timePostOperations: WarehousePostOperation<TimePostOperation>
        get() = mapOf()
    override val datePostOperations: WarehousePostOperation<DatePostOperation>
        get() = mapOf()
    override val timestampPostOperations: WarehousePostOperation<TimestampPostOperation>
        get() = mapOf()
}
