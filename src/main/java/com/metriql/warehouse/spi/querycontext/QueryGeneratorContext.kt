package com.metriql.warehouse.spi.querycontext

import com.google.common.base.CaseFormat
import com.metriql.report.ReportType
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationRecipeQuery.SegmentationMaterialize
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttributeFetcher
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.DimensionName
import com.metriql.service.model.IModelService
import com.metriql.service.model.MeasureName
import com.metriql.service.model.Model
import com.metriql.service.model.Model.Measure.AggregationType.COUNT
import com.metriql.service.model.Model.Measure.MeasureValue.Column
import com.metriql.service.model.Model.Measure.Type.COLUMN
import com.metriql.service.model.ModelDimension
import com.metriql.service.model.ModelMeasure
import com.metriql.service.model.ModelName
import com.metriql.service.model.ModelRelation
import com.metriql.service.model.RelationName
import com.metriql.service.model.getMappingDimensionIfValid
import com.metriql.util.MetriqlException
import com.metriql.util.TextUtil
import com.metriql.util.toSnakeCase
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.services.RecipeQuery
import io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND
import java.util.LinkedHashMap
import java.util.concurrent.ConcurrentHashMap

val TOTAL_ROWS_MEASURE = Model.Measure("__total_rows", null, null, null, COLUMN, Column(COUNT, null))
typealias ReportExecutor = (ProjectAuth, ReportType, RecipeQuery) -> String

interface DependencyFetcher {
    fun fetch(context: IQueryGeneratorContext, model: ModelName): Recipe.Dependencies
}

class QueryGeneratorContext(
    override val auth: ProjectAuth,
    override val datasource: DataSource,
    override val modelService: IModelService,
    override val renderer: JinjaRendererService,
    override val reportExecutor: ReportExecutor?,
    override val userAttributeFetcher: UserAttributeFetcher?,
    override val dependencyFetcher: DependencyFetcher?,
    override val comments: MutableList<String> = mutableListOf(),
    val variables: Map<String, Any>? = null,
) : IQueryGeneratorContext {
    override val viewModels = LinkedHashMap<ModelName, String>()
    override val referencedDimensions = ConcurrentHashMap<Pair<ModelName, DimensionName>, ModelDimension>()
    override val referencedMeasures = ConcurrentHashMap<Pair<ModelName, MeasureName>, ModelMeasure>()
    override val referencedRelations = ConcurrentHashMap<Pair<ModelName, RelationName>, ModelRelation>()

    val models = ConcurrentHashMap<String, Model>()

    override fun getDependencies(modelName: ModelName): Recipe.Dependencies {
        if (dependencyFetcher == null) {
            throw UnsupportedOperationException()
        }

        return dependencyFetcher.fetch(this, modelName)
    }

    override fun addModel(model: Model) {
        models[model.name] = model
    }

    override fun getModel(modelName: ModelName): Model {
        return models.computeIfAbsent(modelName) {
            modelService.getModel(auth, modelName)
                ?: throw MetriqlException("Model '$modelName' not found", NOT_FOUND)
        }
    }

    override fun getAggregatesForModel(target: Model.Target, reportType: ReportType): List<Triple<ModelName, String, SegmentationMaterialize>> {
        val allModels = modelService.list(auth, target = target)

        return allModels.filter { it.target == target }
            .flatMap { model ->
                model.materializes?.filter { m -> m.reportType == reportType }
                    ?.map { Triple(model.name, it.name, it.value) } ?: listOf()
            }
    }

    override fun getDimensionAlias(dimensionName: DimensionName, relationName: RelationName?, postOperation: ReportMetric.PostOperation?): String {
        val dimensionLabel = dimensionName.getMappingDimensionIfValid()?.let { TextUtil.toSlug(it.name) } ?: dimensionName
        val prefix = relationName?.let { "${it}_" } ?: ""
        return prefix + if (postOperation != null) {
            "${dimensionLabel}__${postOperation.type.toSnakeCase}_${CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_UNDERSCORE, postOperation.value.name)}"
        } else {
            dimensionLabel
        }
    }

    override fun getMeasureAlias(measureName: MeasureName, relationName: String?): String {
        val name = (relationName?.let { "${it}_" } ?: "") + measureName
        return warehouseBridge.quoteIdentifier(name)
    }

    override fun getMappingDimensions(modelName: ModelName): Model.MappingDimensions {
        return getModel(modelName).mappings
    }

    override fun getModelDimension(dimensionName: DimensionName, modelName: ModelName): ModelDimension {
        val model = getModel(modelName)

        val mappingDimension = dimensionName.getMappingDimensionIfValid()
        val dimensionNameNormalized = (
            if (mappingDimension != null) {
                model.mappings.get(mappingDimension)
                    ?: throw MetriqlException("The dimension `$dimensionName` in model `$modelName` not found", NOT_FOUND)
            } else {
                dimensionName
            }
            )

        val modelDimension = model.dimensions.find { it.name == dimensionNameNormalized }?.let { ModelDimension(model.name, model.target, it) }
            ?: throw MetriqlException("The dimension `$dimensionName` in model `$modelName` not found", NOT_FOUND)

        referencedDimensions[Pair(modelName, dimensionName)] = modelDimension

        return modelDimension
    }

    override fun getModelMeasure(measureName: MeasureName, modelName: ModelName): ModelMeasure {
        val model = getModel(modelName)
        val measure = model.measures.find { it.name == measureName }
            ?: if (measureName == TOTAL_ROWS_MEASURE.name) TOTAL_ROWS_MEASURE else null
                ?: throw MetriqlException("The measure `$measureName` not found in model `${model.name}`", NOT_FOUND)
        val modelMeasure = ModelMeasure(model.name, model.target, measure)
        referencedMeasures[Pair(modelName, measureName)] = modelMeasure
        return modelMeasure
    }

    override fun getRelation(
        sourceModelName: ModelName,
        relationName: RelationName,
    ): ModelRelation {
        val sourceModel = getModel(sourceModelName)
        val relation = sourceModel.relations.find { it.name == relationName } ?: throw MetriqlException(
            "The relation `$relationName` in model `$sourceModelName` not found",
            NOT_FOUND
        )
        val targetModel = getModel(relation.modelName)
        val modelRelation = ModelRelation(sourceModel.target, sourceModel.name, targetModel.target, targetModel.name, relation)
        referencedRelations[Pair(sourceModelName, relationName)] = modelRelation
        return modelRelation
    }

    override fun getSQLReference(
        modelTarget: Model.Target,
        aliasName: String,
        columnName: String?,
        inQueryDimensionNames: List<String>?,
        dateRange: DateRange?,
    ): String {
        val reference = if (columnName != null) {
            datasource.sqlReferenceForTarget(modelTarget, aliasName, columnName)
        } else {
            datasource.sqlReferenceForTarget(modelTarget, aliasName) {
                renderer.render(
                    auth,
                    datasource,
                    it,
                    aliasName,
                    this,
                    inQueryDimensionNames = inQueryDimensionNames,
                    dateRange = dateRange,
                )
            }
        }

        // If target is SQL only return the view alias.
        if (columnName == null && modelTarget.needsWith()) {
            viewModels[aliasName] = reference
            return warehouseBridge.quoteIdentifier(aliasName)
        }

        return reference
    }

    override fun renderSQL(
        sqlRenderable: SQLRenderable,
        modelName: ModelName?,
        inQueryDimensionNames: List<String>?,
        dateRange: DateRange?,
        targetModelName: ModelName?,
        renderAlias: Boolean,
        hook: ((Map<String, Any?>) -> Map<String, Any?>)?,
    ): String {
        return renderer.render(
            auth,
            datasource,
            sqlRenderable,
            modelName,
            this,
            inQueryDimensionNames,
            dateRange,
            targetModelName = targetModelName,
            renderAlias = renderAlias
        )
    }
}
