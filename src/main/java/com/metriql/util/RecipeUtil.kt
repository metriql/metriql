package com.metriql.util

import com.metriql.service.dataset.DiscoverService
import com.metriql.service.dataset.Dataset
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import org.rakam.server.http.HttpServer

object RecipeUtil {
    fun prepareModelsForInstallation(dataSource: DataSource, context: IQueryGeneratorContext, datasets: List<Dataset>): List<Dataset> {
        checkResourceNames(datasets)

        val fieldUniquenessErrors = datasets.mapNotNull { model ->
            val collidingFieldNames = model.dimensions.map { it.name }.intersect(model.measures.map { it.name }.toSet())
            if (collidingFieldNames.isEmpty()) {
                null
            } else {
                "`${model.name}`: Field names must be unique, duplicate fields found: ${collidingFieldNames.map { "`$it`" }}}"
            }
        }

        // check for relation targets
        val relationErrors = datasets.flatMap { model ->
            model.relations.filter { relation -> !datasets.any { it.name == relation.datasetName } }.map {
                "`${it.datasetName}` dataset not found for relation ${model.name}.${it.name}"
            }
        }

        val allErrors = fieldUniquenessErrors + relationErrors

        if (allErrors.isNotEmpty()) {
            throw MetriqlException(allErrors.map { HttpServer.JsonAPIError.title(it) }, mapOf(), HttpResponseStatus.BAD_REQUEST)
        }

        val discoverService = DiscoverService(dataSource)

        // add all models to context in order to make it available in discoverService.discoverDimensionFieldTypes called below
        datasets.forEach { context.addModel(it) }

        return datasets.map { model ->
            val discoveredDimensions = try {
                discoverService.discoverDimensionFieldTypes(context, model.name, model.target, model.dimensions)
            } catch (e: MetriqlException) {
                model.dimensions
            }

            val dimensions = discoveredDimensions.map {
                it.copy(postOperations = DiscoverService.fillDefaultPostOperations(it, dataSource.warehouse.bridge))
            }

            val modelWithFieldTypes = model.copy(target = dataSource.fillDefaultsToTarget(model.target), dimensions = dimensions)

            // override new model
            context.addModel(modelWithFieldTypes)

            modelWithFieldTypes
        }
    }

    private fun checkResourceNames(datasets: List<Dataset>) {
        val invalidModels = datasets.filter { !TextUtil.resourceRegex.matches(it.name) }
        if (invalidModels.isNotEmpty()) {
            throw MetriqlException("Invalid model names '${invalidModels.map { it.name }}'. Must be lower case and can only contain '_'", HttpResponseStatus.BAD_REQUEST)
        }

        datasets.forEach { model ->
            val invalidRelations = model.relations.filter { !TextUtil.resourceRegex.matches(it.name) }
            if (invalidRelations.isNotEmpty()) {
                throw MetriqlException(
                    "Invalid relation names '${invalidRelations.map { it.name }}', in model '${model.name}}'. Must be lower case and can only contain '_'",
                    HttpResponseStatus.BAD_REQUEST
                )
            }
            val invalidMeasures = model.measures.filter { !TextUtil.resourceRegex.matches(it.name) }
            if (invalidMeasures.isNotEmpty()) {
                throw MetriqlException(
                    "Invalid measure names '${invalidMeasures.map { it.name }}', in model '${model.name}}'. Must be lower case and can only contain '_'",
                    HttpResponseStatus.BAD_REQUEST
                )
            }

            val invalidDimensions = model.dimensions.filter { !TextUtil.resourceRegex.matches(it.name) }
            if (invalidDimensions.isNotEmpty()) {
                throw MetriqlException(
                    "Invalid dimension names '${invalidDimensions.map { it.name }}', in model '${model.name}}'. Must be lower case and can only contain '_'",
                    HttpResponseStatus.BAD_REQUEST
                )
            }
        }
    }
}
