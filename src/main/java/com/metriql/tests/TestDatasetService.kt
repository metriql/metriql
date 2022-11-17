package com.metriql.tests

import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.IDatasetService
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.DatasetName

open class TestDatasetService(private var datasets: List<Dataset> = listOf()) : IDatasetService {

    override fun list(auth: ProjectAuth, target: Dataset.Target?) = datasets

    override fun getDataset(auth: ProjectAuth, datasetName: DatasetName) = datasets.find { datasetName.toRegex().matches(it.name) }

    override fun update(auth: ProjectAuth) {
        TODO("not implemented")
    }
}
