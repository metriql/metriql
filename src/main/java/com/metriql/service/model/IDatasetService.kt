package com.metriql.service.model

import com.metriql.service.auth.ProjectAuth

interface IDatasetService {
    fun list(auth: ProjectAuth, target: Dataset.Target? = null): List<Dataset>
    fun getDataset(auth: ProjectAuth, datasetName: DatasetName): Dataset?
    fun update(auth: ProjectAuth)

    fun listDatasetNames(auth: ProjectAuth): List<DatasetLabel> {
        return list(auth).map { DatasetLabel(it.name, it.label ?: it.name) }.toList()
    }

    data class DatasetLabel(val name: String, val label: String)
}
