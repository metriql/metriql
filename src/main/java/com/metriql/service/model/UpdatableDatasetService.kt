package com.metriql.service.model

import com.metriql.service.auth.ProjectAuth

class UpdatableDatasetService(val modelService: IDatasetService?, private val modelsFetcher: () -> List<Model>) : IDatasetService {

    @Volatile
    var currentModels = modelsFetcher.invoke()

    @Synchronized
    override fun update(auth: ProjectAuth) {
        currentModels = modelsFetcher.invoke()
    }

    /*
        Returns the available models within the same recipe
     */
    override fun list(auth: ProjectAuth, target : Model.Target?): List<Model> {
        return currentModels
    }

    /*
        Looks up the current model list first and fallbacks to the modelService if it's set
     */
    override fun getDataset(auth: ProjectAuth, modelName: ModelName): Model? {
        val regex = modelName.toRegex()
        return list(auth).find { regex.matches(it.name) } ?: modelService?.getDataset(auth, modelName)
    }
}
