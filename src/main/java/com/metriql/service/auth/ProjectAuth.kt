package com.metriql.service.auth

import com.metriql.warehouse.spi.WarehouseAuth
import java.time.ZoneId

open class ProjectAuth(
    val userId: Int,
    val projectId: Int,
    val isOwner: Boolean,
    val isSuperuser: Boolean,
    val email: String?,
    val permissions: List<Int>?,
    val timezone: ZoneId?
) {
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
                timezone = timezone
            )
        }

        fun singleProject(timezone: ZoneId? = null): ProjectAuth {
            return systemUser(-1, timezone = timezone)
        }
    }
}
