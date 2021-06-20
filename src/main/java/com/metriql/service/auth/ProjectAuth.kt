package com.metriql.service.auth

import com.fasterxml.jackson.annotation.JsonIgnore
import com.metriql.warehouse.spi.WarehouseAuth
import java.time.ZoneId

class ProjectAuth(
    val userId: Int,
    val projectId: Int,
    val isOwner: Boolean,
    val isSuperuser: Boolean,
    val email: String?,
    val permissions: List<Int>?,
    val attributes: Map<String, Any?>?,
    val timezone: ZoneId?
) {
    @JsonIgnore
    fun warehouseAuth() = WarehouseAuth(projectId, userId, timezone)

    companion object {
        fun systemUser(projectId: Int, userId: Int = -1, timezone: ZoneId? = null): ProjectAuth {
            return ProjectAuth(
                userId,
                projectId,
                isOwner = true,
                isSuperuser = true,
                email = null,
                permissions = null,
                attributes = null,
                timezone = timezone
            )
        }

        fun singleProject(timezone: ZoneId? = null): ProjectAuth {
            return systemUser(-1, timezone = timezone)
        }
    }
}
