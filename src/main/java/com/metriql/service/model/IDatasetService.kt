package com.metriql.service.model

import com.metriql.service.auth.ProjectAuth

interface IDatasetService {
    fun list(auth: ProjectAuth, target: Model.Target? = null): List<Model>
    fun getDataset(auth: ProjectAuth, modelName: ModelName): Model?
    fun update(auth: ProjectAuth)

    fun listDatasetNames(auth: ProjectAuth): List<DatasetName> {
        return list(auth).map { DatasetName(it.name, it.label ?: it.name) }.toList()
    }

    data class DatasetName(val name: String, val label: String)
}
