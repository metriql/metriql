package com.metriql.deployment

import com.metriql.UserContext
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.warehouse.spi.DataSource
import io.trino.server.security.BasicAuthCredentials

interface Deployment {
    val authType : AuthType

    fun getModelService(): IModelService
    fun logStart()
    fun getDataSource(auth: ProjectAuth): DataSource
    fun getAuth(context: UserContext): ProjectAuth

    enum class AuthType {
        NONE, USERNAME_PASS, ACCESS_TOKEN
    }
}
