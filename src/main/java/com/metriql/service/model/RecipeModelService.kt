package com.metriql.service.model

import com.metriql.report.Recipe
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge

class RecipeModelService(val modelService: IModelService?, val recipe: Recipe, val recipeId: Int, val bridge: WarehouseMetriqlBridge) : IModelService {
    override fun getRecipeDependencies(projectId: Int, recipeId: Int): Recipe.Dependencies {
        return (if (recipeId == this.recipeId) recipe.dependencies else null) ?: modelService?.getRecipeDependencies(projectId, recipeId) ?: Recipe.Dependencies(null)
    }

    override fun list(auth: ProjectAuth): List<Model> {
        return recipe.models?.map { it.toModel(recipe.packageName ?: "", bridge, recipeId) } ?: listOf()
    }

    override fun getModel(auth: ProjectAuth, modelName: ModelName): Model? {
        val regex = modelName.toRegex()
        return list(auth).find { regex.matches(it.name) } ?: modelService?.getModel(auth, modelName)
    }

    override fun delete(auth: ProjectAuth, id: List<Int>) = throw UnsupportedOperationException()
}
