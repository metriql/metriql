package com.metriql.warehouse.spi.function

import com.metriql.util.MetriqlException
import io.netty.handler.codec.http.HttpResponseStatus

typealias WarehousePostOperation<T> = Map<T, String>

fun <T> getRequiredPostOperation(functions: WarehousePostOperation<T>, function: T): String {
    return functions[function] ?: throw MetriqlException("$function post operation is not implemented", HttpResponseStatus.BAD_REQUEST)
}

interface WarehouseTimeframes {
    val timePostOperations: WarehousePostOperation<TimePostOperation>
    val datePostOperations: WarehousePostOperation<DatePostOperation>
    val timestampPostOperations: WarehousePostOperation<TimestampPostOperation>
}
