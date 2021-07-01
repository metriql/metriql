package com.metriql.service.task

import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.service.auth.ProjectAuth
import com.metriql.util.MetriqlException
import com.metriql.util.Scheduler
import com.metriql.util.logging.ContextLogger
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.Collections
import java.util.Timer
import java.util.TimerTask
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import javax.inject.Inject

class TaskQueueService @Inject constructor(private val executor: TaskExecutorService) {
    private val taskTickets = Collections.synchronizedMap(LinkedHashMap<UUID, Task<*, *>>())
    private val timer = Timer()

    init {
        timer.schedule(
            object : TimerTask() {
                override fun run() {
                    val currentTimeMillis = System.currentTimeMillis()
                    taskTickets.forEach {
                        if (it.value.getLastAccessedAt() == null || (currentTimeMillis - it.value.getLastAccessedAt()!! > ORPHAN_TASK_AGE_MILLIS)) {
                            if (!it.value.isDone()) {
                                ContextLogger.log(logger, "Cancelling orphan task: ${it.key}", ProjectAuth.systemUser(0, 0))
                                it.value.cancel()
                            }
                        }
                    }
                }
            },
            ORPHAN_TASK_AGE_MILLIS, ORPHAN_TASK_AGE_MILLIS
        ) // Check with 30 seconds of periods, skip first 5

        /*
        * Add shutdown hook to cancel all tasks on sigterm
        * Kubernetes sends SIGTERM on rolling update, hook for sigterm and cancel the jobs
        * See: https://cloud.google.com/blog/products/gcp/kubernetes-best-practices-terminating-with-grace
        * */
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                // Drain the waiting queue and cancel
                val waitingJobs = mutableListOf<Runnable>()

                // Get the running and idle tasks
                taskTickets.values
                    .filter { it.status == Task.Status.RUNNING || it.status == Task.Status.QUEUED }
                    .forEach {
                        waitingJobs.add(it)
                    }

                logger.info("Killing all the tasks (${waitingJobs.size}), exiting..")

                waitingJobs.forEach {
                    if (it is Task<*, *>) {
                        val queryTask = it as QueryTask
                        queryTask.setResult(QueryResult.errorResult("We're currently operating an update. Please try again in 30 seconds."))
                    }
                }
            }
        })
    }

    fun status(id: UUID): Task.TaskTicket<*> {
        val task = taskTickets[id] ?: throw MetriqlException(HttpResponseStatus.NOT_FOUND)
        return task.taskTicket()
    }

    fun currentTasks(includeResponse: Boolean = false, filterStatus : Task.Status? = null): List<Task.TaskTicket<out Any?>> {
        val allTasks = taskTickets.map { it.value.taskTicket(includeResponse) }
        return if(filterStatus == null) {
            allTasks
        } else {
            allTasks.filter { it.status == filterStatus }
        }
    }

    fun cancel(id: UUID) {
        val task = taskTickets[id] ?: throw MetriqlException("Task not found", HttpResponseStatus.NOT_FOUND)
        task.cancel()
    }

    fun <T, K> execute(runnable: Task<T, K>) {
        if (runnable.getId() != null) {
            // if the task has an id, it's already being processed
            return
        }

        val taskId = UUID.randomUUID()
        runnable.setId(taskId)

        if (runnable.status != Task.Status.FINISHED) {
            // if it's a completed task, don't bother the executor
            executor.execute(runnable)
        }

        taskTickets[taskId] = runnable
        // if we reached the limit, remove the first item from the task list
        if (taskTickets.size >= MAXIMUM_ITEMS_IN_TASK_LIST) {
            val iterator = taskTickets.iterator()
            val item = iterator.next()
            if (!item.value.isDone()) {
                throw IllegalStateException()
            }
            iterator.remove()
        }
    }

    fun <T, K> execute(runnable: Task<T, K>, initialWaitInSeconds: Long): CompletableFuture<Task<T, K>> {
        if (runnable.status == Task.Status.FINISHED) {
            return CompletableFuture.completedFuture(runnable)
        }

        execute(runnable)

        val future = CompletableFuture<Task<T, K>>()
        runnable.onFinish { future.complete(runnable) }

        if (initialWaitInSeconds > 0) {
            Scheduler.executor.schedule(
                {
                    if (!future.isDone) {
                        future.complete(runnable)
                    }
                },
                initialWaitInSeconds, TimeUnit.SECONDS
            )
        }

        return future
    }

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)
        private const val ORPHAN_TASK_AGE_MILLIS = 1000 * 60 * 1L
        private const val MAXIMUM_ITEMS_IN_TASK_LIST = 1000
    }
}
