package com.metriql.interfaces

import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IDatasetService
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName

open class TestDatasetService(private var models: List<Model> = listOf()) : IDatasetService {

    override fun list(auth: ProjectAuth, target : Model.Target?) = models

    override fun getDataset(auth: ProjectAuth, modelName: ModelName) = models.find { modelName.toRegex().matches(it.name) }

    override fun update(auth: ProjectAuth) {
        TODO("not implemented")
    }
}
