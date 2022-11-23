package com.metriql.deployment

import com.metriql.UserContext
import com.metriql.report.QueryTaskGenerator
import com.metriql.report.jinja.JinjaApps
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.IDatasetService
import com.metriql.service.suggestion.SuggestionCacheService
import com.metriql.warehouse.spi.DataSource

interface Deployment {
    val authType: AuthType

    fun getDatasetService(): IDatasetService
    fun logStart() {}
    fun getDataSource(auth: ProjectAuth): DataSource
    fun getApps(auth: ProjectAuth) = JinjaApps(listOf())
    fun getAuth(context: UserContext): ProjectAuth

    fun getTaskGenerators(): List<QueryTaskGenerator> = listOf()
    fun getCacheService(): SuggestionCacheService

    enum class AuthType {
        NONE, USERNAME_PASS, ACCESS_TOKEN
    }
}
