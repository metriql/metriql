package com.metriql.service.auth

import com.fasterxml.jackson.annotation.JsonIgnore
import com.metriql.warehouse.spi.WarehouseAuth
import java.time.ZoneId

data class ProjectAuth(
    val userId: Any,
    val projectId: Int,
    val isOwner: Boolean,
    val isSuperuser: Boolean,
    val email: String?,
    val permissions: List<Int>?,
    val attributes: Map<String, Any?>?,
    val timezone: ZoneId?,
    val source: String?,
) {
    @JsonIgnore
    fun warehouseAuth() = WarehouseAuth(projectId, userId, timezone, source)

    companion object {
        fun systemUser(projectId: Int, userId: Any = -1, timezone: ZoneId? = null): ProjectAuth {
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
            return systemUser(-1, timezone = timezone)
        }
    }
}
