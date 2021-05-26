package com.metriql.service.model

import com.metriql.report.Recipe
import com.metriql.service.auth.ProjectAuth

interface IModelService {
    fun getRecipeDependencies(projectId: Int, recipeId: Int): Recipe.Dependencies
    fun list(auth: ProjectAuth): List<Model>
    fun getModel(auth: ProjectAuth, modelName: ModelName): Model?
    @Deprecated("It's being used for stale models")
    fun delete(auth: ProjectAuth, id: List<Int>)
}
