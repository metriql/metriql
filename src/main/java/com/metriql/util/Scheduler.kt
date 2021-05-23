package com.metriql.util

import com.google.common.util.concurrent.ThreadFactoryBuilder
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

object Scheduler {
    val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(
        5,
        ThreadFactoryBuilder()
            .setNameFormat("generic-scheduler-pool")
            .setDaemon(true)
            .build()
    )
}
