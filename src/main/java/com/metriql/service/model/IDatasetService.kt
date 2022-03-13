package com.metriql.service.model

import com.metriql.service.auth.ProjectAuth

interface IDatasetService {
    fun list(auth: ProjectAuth, target : Model.Target? = null): List<Model>
    fun getDataset(auth: ProjectAuth, modelName: ModelName): Model?
    fun update(auth: ProjectAuth)

    fun listDatasetNames(auth : ProjectAuth) : Set<String> {
        return list(auth).map { it.name }.toSet()
    }
}
