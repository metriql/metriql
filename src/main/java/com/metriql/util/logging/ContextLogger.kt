package com.metriql.util.logging

import com.metriql.service.auth.ProjectAuth
import java.util.logging.Level
import java.util.logging.Logger

// Sets user, project-ids to thread-context and removes after logging
object ContextLogger {

    fun log(logger: Logger, message: String, auth: ProjectAuth, level: Level? = Level.INFO, throwable: Throwable? = null) {
        log(logger, message, throwable, level, auth.userId.toString(), auth.projectId.toString())
    }

    private fun log(logger: Logger, message: String, throwable: Throwable?, level: Level? = Level.INFO, userId: String, projectId: String) {
        logger.log(level, message, throwable)
    }
}
