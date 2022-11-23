package com.metriql.warehouse.spi.querycontext

import com.google.common.base.CaseFormat
import com.metriql.report.ReportType
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationMaterialize
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttributeFetcher
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.Dataset.Measure.AggregationType.COUNT
import com.metriql.service.dataset.Dataset.Measure.MeasureValue.Column
import com.metriql.service.dataset.Dataset.Measure.Type.COLUMN
import com.metriql.service.dataset.DatasetName
import com.metriql.service.dataset.DimensionName
import com.metriql.service.dataset.IDatasetService
import com.metriql.service.dataset.MeasureName
import com.metriql.service.dataset.ModelDimension
import com.metriql.service.dataset.ModelMeasure
import com.metriql.service.dataset.ModelRelation
import com.metriql.service.dataset.RelationName
import com.metriql.service.dataset.getMappingDimensionIfValid
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.jinja.SQLRenderable
import com.metriql.util.MetriqlException
import com.metriql.util.TextUtil
import com.metriql.util.toSnakeCase
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.services.RecipeQuery
import io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND
import java.util.LinkedHashMap
import java.util.concurrent.ConcurrentHashMap

val TOTAL_ROWS_MEASURE = Dataset.Measure("__total_rows", null, null, null, COLUMN, Column(COUNT, null))
typealias ReportExecutor = (ProjectAuth, ReportType, RecipeQuery) -> String

interface DependencyFetcher {
    fun fetch(context: IQueryGeneratorContext, model: DatasetName): Recipe.Dependencies
}

class QueryGeneratorContext(
    override val auth: ProjectAuth,
    override val datasource: DataSource,
    override val datasetService: IDatasetService,
    override val renderer: JinjaRendererService,
    override val reportExecutor: ReportExecutor?,
    override val userAttributeFetcher: UserAttributeFetcher?,
    override val dependencyFetcher: DependencyFetcher?,
    override val comments: MutableList<String> = mutableListOf(),
    val variables: Map<String, Any>? = null,
) : IQueryGeneratorContext {
    override val viewModels = LinkedHashMap<DatasetName, String>()
    override val aliases = LinkedHashMap<Pair<DatasetName, RelationName?>, String>()
    override val referencedDimensions = ConcurrentHashMap<Pair<DatasetName, DimensionName>, ModelDimension>()
    override val referencedMeasures = ConcurrentHashMap<Pair<DatasetName, MeasureName>, ModelMeasure>()
    override val referencedRelations = ConcurrentHashMap<Pair<DatasetName, RelationName>, ModelRelation>()

    val models = ConcurrentHashMap<String, Dataset>()

    override fun getDependencies(datasetName: DatasetName): Recipe.Dependencies {
        if (dependencyFetcher == null) {
            throw UnsupportedOperationException()
        }

        return dependencyFetcher.fetch(this, datasetName)
    }

    override fun addModel(dataset: Dataset) {
        models[dataset.name] = dataset
    }

    override fun getModel(datasetName: DatasetName): Dataset {
        return models.computeIfAbsent(datasetName) {
            datasetService.getDataset(auth, datasetName)
                ?: throw MetriqlException("Model '$datasetName' not found", NOT_FOUND)
        }
    }

    override fun getAggregatesForModel(target: Dataset.Target, reportType: ReportType): List<Triple<DatasetName, String, SegmentationMaterialize>> {
        return listOf()
        //        val allModels = datasetService.list(auth, target = target)
        //        return allModels.filter { it.target == target }
        //            .flatMap { model ->
        //                model.materializes?.filter { m -> m.reportType == reportType }
        //                    ?.map { Triple(model.name, it.name, it.value) } ?: listOf()
        //            }
    }

    override fun getDimensionAlias(dimensionName: DimensionName, relationName: RelationName?, timeframe: ReportMetric.Timeframe?): String {
        val dimensionLabel = dimensionName.getMappingDimensionIfValid()?.let { TextUtil.toSlug(it.name) } ?: dimensionName
        val prefix = relationName?.let { "${it}_" } ?: ""
        return prefix + if (timeframe != null) {
            "${dimensionLabel}__${timeframe.type.toSnakeCase}_${CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_UNDERSCORE, timeframe.value.name)}"
        } else {
            dimensionLabel
        }
    }

    override fun getMeasureAlias(measureName: MeasureName, relationName: String?): String {
        val name = (relationName?.let { "${it}_" } ?: "") + measureName
        return warehouseBridge.quoteIdentifier(name)
    }

    override fun getMappingDimensions(datasetName: DatasetName): Dataset.MappingDimensions {
        return getModel(datasetName).mappings
    }

    override fun getModelDimension(dimensionName: DimensionName, datasetName: DatasetName): ModelDimension {
        val model = getModel(datasetName)

        val mappingDimension = dimensionName.getMappingDimensionIfValid()
        val dimensionNameNormalized = (
            if (mappingDimension != null) {
                model.mappings.get(mappingDimension)
                    ?: throw MetriqlException("The $mappingDimension mapping dimension in model `$datasetName` not found", NOT_FOUND)
            } else {
                dimensionName
            }
            )

        val modelDimension = model.dimensions.find { it.name == dimensionNameNormalized }?.let { ModelDimension(model.name, model.target, it) }
            ?: throw MetriqlException("The dimension `$dimensionName` in model `$datasetName` not found", NOT_FOUND)

        referencedDimensions[Pair(datasetName, dimensionName)] = modelDimension

        return modelDimension
    }

    override fun getModelMeasure(measureName: MeasureName, datasetName: DatasetName): ModelMeasure {
        val model = getModel(datasetName)
        val measure = model.measures.find { it.name == measureName }
            ?: if (measureName == TOTAL_ROWS_MEASURE.name) TOTAL_ROWS_MEASURE else null
                ?: throw MetriqlException("The measure `$measureName` not found in model `${model.name}`", NOT_FOUND)
        val modelMeasure = ModelMeasure(model.name, model.target, measure)
        referencedMeasures[Pair(datasetName, measureName)] = modelMeasure
        return modelMeasure
    }

    override fun getRelation(
        sourceDatasetName: DatasetName,
        relationName: RelationName,
    ): ModelRelation {
        val sourceModel = getModel(sourceDatasetName)
        val relation = sourceModel.relations.find { it.name == relationName } ?: throw MetriqlException(
            "The relation `$relationName` in model `$sourceDatasetName` not found",
            NOT_FOUND
        )
        val targetModel = getModel(relation.datasetName)
        val modelRelation = ModelRelation(sourceModel.target, sourceModel.name, targetModel.target, targetModel.name, relation)
        referencedRelations[Pair(sourceDatasetName, relationName)] = modelRelation
        return modelRelation
    }

    override fun getSQLReference(
        datasetTarget: Dataset.Target,
        aliasName: String,
        modelName: String,
        columnName: String?,
        inQueryDimensionNames: List<String>?,
        dateRange: DateRange?,
    ): String {
        val reference = if (columnName != null) {
            datasource.sqlReferenceForTarget(datasetTarget, aliasName, columnName)
        } else {
            datasource.sqlReferenceForTarget(datasetTarget, aliasName) {
                renderer.render(
                    auth,
                    it,
                    aliasName,
                    this,
                    inQueryDimensionNames = inQueryDimensionNames,
                    sourceDatasetName = modelName,
                    dateRange = dateRange,
                )
            }
        }

        // If target is SQL only return the view alias.
        if (columnName == null && datasetTarget.needsWith()) {
            viewModels[aliasName] = reference
            return warehouseBridge.quoteIdentifier(aliasName)
        }

        return reference
    }

    override fun renderSQL(
        sqlRenderable: SQLRenderable,
        alias: String?,
        datasetName: DatasetName?,
        inQueryDimensionNames: List<String>?,
        dateRange: DateRange?,
        renderAlias: Boolean,
        extraContext: Map<String, Any>,
        hook: ((Map<String, Any?>) -> Map<String, Any?>)?,
    ): String {
        return renderer.render(
            auth,
            sqlRenderable,
            alias,
            this,
            inQueryDimensionNames,
            dateRange,
            sourceDatasetName = datasetName,
            extraContext = extraContext,
            renderAlias = renderAlias
        )
    }
}
