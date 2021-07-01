package com.metriql.service.task

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metriql.service.audit.MetriqlEvents
import com.metriql.util.MetriqlEventBus
import io.sentry.Sentry
import io.sentry.SpanStatus
import java.util.concurrent.BlockingQueue
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import javax.inject.Inject

class TaskExecutorService constructor(val poolSize: Int) {
    @Inject
    constructor() : this(MAX_POOL_SIZE)

    private val workQueue: BlockingQueue<Runnable>
    private val threadPoolExecutor: ThreadPoolExecutor
    private val threadFactory: ThreadFactory

    init {
        //  Default choices are SynchronousQueue for multi-threaded pools and LinkedBlockingQueue for single-threaded pools.
        this.workQueue = SynchronousQueue()
        this.threadFactory = ThreadFactoryBuilder()
            .setNameFormat("metriql-task")
            .setUncaughtExceptionHandler { _, e ->
                logger.log(Level.SEVERE, "Uncaught exception detected in query executor thread", e)
            }
            .build()

        this.threadPoolExecutor = object : ThreadPoolExecutor(
            INITIAL_POOL_SIZE, poolSize, KEEP_ALIVE_TIME.toLong(), KEEP_ALIVE_TIME_UNIT,
            this.workQueue, this.threadFactory
        ) {
            override fun afterExecute(r: Runnable?, t: Throwable?) {
                super.afterExecute(r, t)
                if (r is Task<*, *> && t != null) {
                    if (r.status == Task.Status.RUNNING) {
                        r.cancel()
                    }
                    MetriqlEventBus.publish(MetriqlEvents.UnhandledTaskException(t, r))
                } else if (t != null) {
                    MetriqlEventBus.publish(MetriqlEvents.InternalException(t, null, null))
                }
            }
        }
    }

    fun execute(task: Task<*, *>) {
        this.threadPoolExecutor.execute {
            val span = Sentry.getSpan()?.startChild("query") ?: Sentry.startTransaction("query")
            span.description = task.getId().toString()
            span.setTag("user", task.user.toString())
            span.setTag("project", task.projectId.toString())

            try {
                task.run()
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Unable to run task ${task.getId()}", e)
                task.cancel()
                span.status = SpanStatus.INTERNAL_ERROR
                span.throwable = e
            } finally {
                span.finish()
            }
        }
    }

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)
        private const val INITIAL_POOL_SIZE = 64

        // the queries mostly use the JDBC api which is sync so we need a big thread pool
        private const val MAX_POOL_SIZE = 512
        private const val KEEP_ALIVE_TIME = 60
        private val KEEP_ALIVE_TIME_UNIT = TimeUnit.SECONDS
    }
}
