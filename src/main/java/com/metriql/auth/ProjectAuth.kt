package com.metriql.auth

import com.metriql.warehouse.spi.WarehouseAuth
import java.time.ZoneId

data class ProjectAuth(
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
        fun systemUser(projectId: Int, userId: Int = -1): ProjectAuth {
            return ProjectAuth(
                userId,
                projectId,
                isOwner = true,
                isSuperuser = true,
                email = null,
                permissions = null,
                timezone = null
            )
        }

        fun singleProject(): ProjectAuth {
            return systemUser(-1)
        }
    }
}
