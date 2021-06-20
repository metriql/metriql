package com.metriql.service.model

import com.metriql.report.Recipe
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge

class RecipeModelService(val modelService: IModelService?, private val recipeFetcher: () -> Recipe, val recipeId: Int, val bridge: WarehouseMetriqlBridge) : IModelService {
    @Volatile
    var currentRecipe = recipeFetcher.invoke()

    override fun getRecipeDependencies(projectId: Int, recipeId: Int): Recipe.Dependencies {
        return (if (recipeId == this.recipeId) currentRecipe.dependencies else null)
            ?: modelService?.getRecipeDependencies(projectId, recipeId) ?: Recipe.Dependencies(null)
    }

    @Synchronized
    override fun update() {
        currentRecipe = recipeFetcher.invoke()
    }

    override fun list(auth: ProjectAuth): List<Model> {
        return currentRecipe.models?.map { it.toModel(currentRecipe.packageName ?: "", bridge, recipeId) } ?: listOf()
    }

    override fun getModel(auth: ProjectAuth, modelName: ModelName): Model? {
        val regex = modelName.toRegex()
        return list(auth).find { regex.matches(it.name) } ?: modelService?.getModel(auth, modelName)
    }

    override fun delete(auth: ProjectAuth, id: List<Int>) = throw UnsupportedOperationException()
}
