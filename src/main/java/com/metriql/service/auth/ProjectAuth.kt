package com.metriql.service.auth

import java.time.ZoneId

data class ProjectAuth(
    val userId: Any,
    val projectId: String,
    val isOwner: Boolean,
    val isSuperuser: Boolean,
    val email: String?,
    val permissions: List<Int>?,
    val attributes: Map<String, Any?>?,
    val timezone: ZoneId?,
    val source: String?,
    val credentials: Map<String, String>? = null
) {

    companion object {
        fun systemUser(projectId: String, userId: Any = -1, timezone: ZoneId? = null): ProjectAuth {
            return ProjectAuth(
                userId,
                projectId,
                isOwner = true,
                isSuperuser = true,
                email = null,
                permissions = null,
                attributes = null,
                source = "system",
                timezone = timezone
            )
        }

        fun singleProject(timezone: ZoneId? = null): ProjectAuth {
            return systemUser("", timezone = timezone)
        }

        const val PASSWORD_CREDENTIAL = "password"
    }
}
