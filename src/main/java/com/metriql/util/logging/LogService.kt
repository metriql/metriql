package com.metriql.util.logging

import com.google.common.eventbus.Subscribe
import com.metriql.service.audit.MetriqlEvents
import com.metriql.util.MetriqlEventBus
import com.metriql.util.MetriqlException
import com.metriql.util.serializableName
import io.sentry.Sentry
import io.sentry.SentryEvent
import io.sentry.SentryLevel
import io.sentry.SentryOptions
import io.sentry.protocol.Message
import io.sentry.protocol.Request
import org.rakam.server.http.RakamHttpRequest

object LogService {
    var logActive = false
    var release: String? = null

    init {
        MetriqlEventBus.register(this)
    }

    fun configure(config: LogConfig) {
        logActive = config.logActive
        release = config.release

        if (logActive) {
            Sentry.init { options: SentryOptions ->
                options.dsn = config.sentryDSN
                // To set a uniform sample rate
                options.tracesSampleRate = 1.0
            }
        }
    }

    @Subscribe
    fun logUnhandledTaskException(event: MetriqlEvents.UnhandledTaskException) {
        if (!logActive) {
            return
        }

        val sentryEvent = SentryEvent()
        sentryEvent.level = SentryLevel.WARNING
        sentryEvent.throwable = event.e

        sentryEvent.setTag("project-id", "${event.task.projectId}")
        sentryEvent.setTag("user", "${event.task.user}")
        sentryEvent.setTag("task-status", event.task.status.serializableName)
        sentryEvent.setTag("task-class", event.task.javaClass.toGenericString())
        sentryEvent.setTag("task-duration", "${event.task.getDuration().seconds} sec")

        if (release != null) {
            sentryEvent.release = release
        }

        Sentry.captureEvent(sentryEvent)
    }

    @Subscribe
    fun logInternalException(event: MetriqlEvents.InternalException) {
        if (!logActive) {
            return
        }

        val sentryEvent = SentryEvent()
        sentryEvent.throwable = event.e
        sentryEvent.setTag("project", event.projectId.toString())
        sentryEvent.setTag("user", event.userId.toString())
        sentryEvent.level = SentryLevel.WARNING

        if (release != null) {
            sentryEvent.release = release
        }

        Sentry.captureException(event.e)
    }

    @Subscribe
    fun logException(event: MetriqlEvents.Exception) {
        when (event.e) {
            is MetriqlException -> logException(event.request, event.e)
        }
    }

    fun logException(request: RakamHttpRequest, e: MetriqlException) {
        logException(request, e, if (e.statusCode.code() in 500..599) SentryLevel.ERROR else SentryLevel.WARNING)
    }

    fun logException(request: RakamHttpRequest, e: Throwable, level: SentryLevel = SentryLevel.ERROR) {
        if (!logActive) { // || e.isUserException) { // TODO: Remove this on a stable release
            return
        }

        val sentryEvent = SentryEvent()
        val req = Request()
        req.headers = request.headers().associate { it.key to it.value }
        req.url = request.uri
        req.method = request.method.name()
        sentryEvent.request = req
        sentryEvent.level = level
        sentryEvent.logger = e.javaClass.name
        if (e !is MetriqlException) {
            sentryEvent.throwable = e
        }
        sentryEvent.message = Message()
        sentryEvent.message.message = e.toString()

        if (release != null) {
            sentryEvent.release = release
        }

        Sentry.captureEvent(sentryEvent)
    }
}
