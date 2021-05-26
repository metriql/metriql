package com.metriql.report

import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.services.RecipeQuery

interface ReportExecutor {
    fun getQuery(auth: ProjectAuth, type: ReportType, options: RecipeQuery): String
}
