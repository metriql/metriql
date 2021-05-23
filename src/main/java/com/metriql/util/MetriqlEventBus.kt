package com.metriql.util

import com.google.common.eventbus.AsyncEventBus
import java.util.concurrent.Executors

object MetriqlEventBus {

    private val bus = AsyncEventBus(Executors.newSingleThreadExecutor())

    fun publish(event: Any) {
        bus.post(event)
    }

    fun register(listener: Any) {
        bus.register(listener)
    }
}
