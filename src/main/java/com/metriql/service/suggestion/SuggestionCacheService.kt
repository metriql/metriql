package com.metriql.service.suggestion

import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.DimensionName
import com.metriql.service.model.ModelName
import java.time.Instant

interface SuggestionCacheService {
    fun getCommon(auth: ProjectAuth, modelName: ModelName, dimensionName: DimensionName): Suggestion?
    fun search(auth: ProjectAuth, modelName: ModelName, dimensionName: DimensionName, filter: String): Suggestion?
    fun set(auth: ProjectAuth, modelName: ModelName, dimensionName: DimensionName, values: List<String>)

    data class Suggestion(val items: List<String>, val lastUpdated: Instant)
}
