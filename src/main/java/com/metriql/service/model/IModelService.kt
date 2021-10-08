package com.metriql.service.model

import com.metriql.service.auth.ProjectAuth

interface IModelService {
    fun list(auth: ProjectAuth): List<Model>
    fun getModel(auth: ProjectAuth, modelName: ModelName): Model?
    fun update(auth: ProjectAuth)
}
