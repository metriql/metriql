package com.metriql.interfaces

import com.metriql.report.Recipe
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName

open class TestModelService(private var models: List<Model> = listOf()) : IModelService {

    override fun getRecipeDependencies(projectId: Int, recipeId: Int): Recipe.Dependencies {
        TODO("not implemented")
    }

    override fun list(auth: ProjectAuth) = models

    override fun getModel(auth: ProjectAuth, modelName: ModelName) = models.find { modelName.toRegex().matches(it.name) }

    override fun delete(auth: ProjectAuth, id: List<Int>) {
        TODO("not implemented") // To change body of created functions use File | Settings | File Templates.
    }
}
