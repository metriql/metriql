package com.metriql.service.suggestion

import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.DimensionName
import com.metriql.service.model.DatasetName
import java.time.Instant

interface SuggestionCacheService {
    fun getCommon(auth: ProjectAuth, datasetName: DatasetName, dimensionName: DimensionName): Suggestion?
    fun search(auth: ProjectAuth, datasetName: DatasetName, dimensionName: DimensionName, filter: String): Suggestion?
    fun set(auth: ProjectAuth, datasetName: DatasetName, dimensionName: DimensionName, values: List<String>)

    data class Suggestion(val items: List<String>, val lastUpdated: Instant)
}
