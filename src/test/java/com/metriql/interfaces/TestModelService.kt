package com.metriql.interfaces

import com.metriql.Recipe
import com.metriql.auth.ProjectAuth
import com.metriql.model.IModelService
import com.metriql.model.Model

open class TestModelService(private var models: List<Model> = listOf()) : IModelService {

    override fun getRecipeDependencies(projectId: Int, recipeId: Int): Recipe.Dependencies {
        TODO("not implemented")
    }

    override fun list(auth: ProjectAuth) = models

    override fun getModel(auth: ProjectAuth, modelName: String) = models.find { it.name == modelName }

    override fun delete(auth: ProjectAuth, id: List<Int>) {
        TODO("not implemented") // To change body of created functions use File | Settings | File Templates.
    }
}
