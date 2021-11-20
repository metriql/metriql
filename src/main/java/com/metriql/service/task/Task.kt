package com.metriql.service.task

import com.fasterxml.jackson.annotation.JsonIgnore
import com.metriql.service.auth.ProjectAuth
import com.metriql.util.UppercaseEnum
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger

/**
 * Task is an abstraction of Runnable,
 * @param T: Result type of the task
 * @param K: Status type of the task
 * @param type: Type of the task
 * @param projectId: Task is executed for project
 * @param userId: Initiated by user, is null if it's a system task such as dashboard update or dbt update tasks
 * @param isBackgroundTask: Is this task a background task, or user initiated?
 * */

abstract class Task<T, K>(val projectId: Int, val user: Any?, val source: String?, private val isBackgroundTask: Boolean) : Runnable {
    private var id: UUID? = null

    private val startedAt = Instant.now()!!
    private var endedAt: Instant? = null
    private var currentStatsCalledAtMillis = System.currentTimeMillis() // Sets when currentStats is called
    private var delegates = mutableListOf<(T?) -> Unit>()
    private var postProcessors = mutableListOf<(T) -> T>()

    @Volatile
    var status = Status.QUEUED
        private set

    private var result: T? = null

    // Can't reach this. inside timer schedule, this is only a proxy method
    private fun cancelTask() {
        this.cancel()
    }

    fun getId() = id

    fun setId(id: UUID) {
        if (this.id != null) {
            throw IllegalArgumentException("Task already have an id.")
        }
        this.id = id
    }

    @Synchronized
    fun setResult(result: T, failed: Boolean = false) {
        if (this.status.isDone) {
            throw IllegalStateException("Task result is already set.")
        }

        var processedResult = result
        for (postProcessor in postProcessors) {
            processedResult = postProcessor.invoke(processedResult)
        }
        this.result = processedResult

        endedAt = Instant.now()
        this.status = if (failed) Status.FAILED else Status.FINISHED

        for (delegate in delegates) {
            // Delegates may perform intensive works.
            // Execute callbacks on a different thread pool within non failing try? block
            try {
                delegate.invoke(result)
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Error running task callback", e)
            }
        }
    }

    fun getDuration(): Duration {
        return Duration.ofSeconds((endedAt ?: Instant.now()).epochSecond - startedAt.epochSecond)
    }

    @Synchronized
    fun onFinish(action: (T?) -> Unit) {
        // Notify immediately in case the task already finished before the delegation
        if (status == Status.FINISHED) {
            try {
                action.invoke(result)
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Error running task callback", e)
            }
        } else {
            delegates.add(action)
        }
    }

    // for tests.
    fun runAndWaitForResult(): T {
        run()
        return result!!
    }

    open fun cancel() {
        endedAt = Instant.now()
        this.status = Status.CANCELED

        delegates.forEach {
            try {
                it.invoke(result)
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Error running task callback", e)
            }
        }
    }

    // should set currentStatsCalledAt
    private fun currentStats(): K {
        currentStatsCalledAtMillis = System.currentTimeMillis()
        return this.getStats()
    }

    protected abstract fun getStats(): K

    data class TaskTicket<T>(
        val id: UUID?,
        val startedAt: Instant,
        val duration: Duration,
        val user: Any?,
        val source: String?,
        val status: Status,
        val update: Any?,
        val result: T?
    )

    fun taskTicket(includeResponse: Boolean = true): TaskTicket<T> {
        if (status != Status.FINISHED && id == null) {
            throw IllegalStateException("Long running tasks can't be serialized without id")
        }

        val duration = Duration.between(startedAt, endedAt ?: Instant.now())
        return TaskTicket(id, startedAt, duration, user, source, status, currentStats(), if (includeResponse) result else null)
    }

    @UppercaseEnum
    enum class Status(val isDone: Boolean) {
        QUEUED(false), RUNNING(false), CANCELED(true), FINISHED(true), FAILED(true)
    }

    fun markAsRunning() {
        if (status != Status.QUEUED) {
            throw IllegalStateException("The task must be in ${Status.QUEUED} status but it's $status")
        }
        status = Status.RUNNING
    }

    fun getLastAccessedAt(): Long? {
        return currentStatsCalledAtMillis
    }

    @Synchronized
    fun addPostProcessor(postProcessor: (T) -> T) {
        if (status == Status.FINISHED) {
            throw IllegalStateException("Can't add new post-processor while task is already finished.")
        }
        postProcessors.add(postProcessor)
    }

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)

        fun <Result, Stat> completedTask(auth: ProjectAuth, id: UUID?, result: Result, stats: Stat, failed: Boolean = false): Task<Result, Stat> {
            val value = object : Task<Result, Stat>(auth.projectId, auth.userId, auth.source, false) {
                override fun run() {}

                override fun getStats() = stats
            }
            if (id != null) {
                value.setId(id)
            }
            value.setResult(result, failed)
            return value
        }
    }
}
