package com.metriql.service.audit

import com.google.common.eventbus.Subscribe
import com.metriql.service.auth.ProjectAuth
import com.metriql.util.MetriqlEventBus
import com.metriql.util.logging.ContextLogger
import java.util.logging.Level
import java.util.logging.Logger
import javax.inject.Inject

class ConsoleAuditService @Inject constructor(val config: AuditConfig) {
    init {
        MetriqlEventBus.register(this)
    }

    @Subscribe
    fun log(event: MetriqlEvents.AuditLog.SQLExecuteEvent) {
        ContextLogger.log(logger, event.query, ProjectAuth.systemUser(event.auth.projectId, event.auth.userId ?: 0), Level.CONFIG) // CONFIG = DEBUG level
    }

    companion object {
        private val logger = Logger.getLogger(ConsoleAuditService::class.java.simpleName)
    }
}
