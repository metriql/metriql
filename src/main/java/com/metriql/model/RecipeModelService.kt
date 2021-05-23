package com.metriql.model

import com.metriql.Recipe
import com.metriql.auth.ProjectAuth
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge

class RecipeModelService(val modelService: IModelService?, val recipe: Recipe, val recipeId: Int, val bridge: WarehouseMetriqlBridge) : IModelService {
    override fun getRecipeDependencies(projectId: Int, recipeId: Int): Recipe.Dependencies {
        return (if (recipeId == this.recipeId) recipe.dependencies else null) ?: modelService?.getRecipeDependencies(projectId, recipeId) ?: Recipe.Dependencies(null)
    }

    override fun list(auth: ProjectAuth): List<Model> {
        return recipe.models?.map { it.toModel(recipe.packageName ?: "", bridge, recipeId) } ?: listOf()
    }

    override fun getModel(auth: ProjectAuth, modelName: String): Model? {
        return list(auth).find { it.name == modelName } ?: modelService?.getModel(auth, modelName)
    }

    override fun delete(auth: ProjectAuth, id: List<Int>) = throw UnsupportedOperationException()
}
