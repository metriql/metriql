package com.metriql.model

import com.metriql.Recipe
import com.metriql.auth.ProjectAuth

interface IModelService {
    fun getRecipeDependencies(projectId: Int, recipeId: Int): Recipe.Dependencies
    fun list(auth: ProjectAuth): List<Model>
    fun getModel(auth: ProjectAuth, modelName: String): Model?

    @Deprecated("It's being used for stale models")
    fun delete(auth: ProjectAuth, id: List<Int>)
}
