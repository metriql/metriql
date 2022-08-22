package com.metriql.warehouse.spi.querycontext

import com.metriql.report.ReportType
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttributeFetcher
import com.metriql.service.auth.UserAttributeValues
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.DimensionName
import com.metriql.service.model.IDatasetService
import com.metriql.service.model.MeasureName
import com.metriql.service.model.Model
import com.metriql.service.model.ModelDimension
import com.metriql.service.model.ModelMeasure
import com.metriql.service.model.ModelName
import com.metriql.service.model.ModelRelation
import com.metriql.service.model.RelationName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.filter.DateRange
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.LinkedHashMap

interface IQueryGeneratorContext {
    val auth: ProjectAuth
    val datasource: DataSource
    val reportExecutor: ReportExecutor?
    val userAttributeFetcher: UserAttributeFetcher?
    val dependencyFetcher: DependencyFetcher?
    val datasetService: IDatasetService
    val renderer: JinjaRendererService
    val referencedDimensions: Map<Pair<ModelName, DimensionName>, ModelDimension>
    val referencedMeasures: Map<Pair<ModelName, MeasureName>, ModelMeasure>
    val referencedRelations: Map<Pair<ModelName, RelationName>, ModelRelation>

    val viewModels: LinkedHashMap<ModelName, String>
    val aliases: LinkedHashMap<Pair<ModelName, RelationName?>, String>
    val comments: MutableList<String>
    val warehouseBridge: WarehouseMetriqlBridge get() = datasource.warehouse.bridge

    fun getDependencies(modelName: ModelName): Recipe.Dependencies
    fun getMappingDimensions(modelName: ModelName): Model.MappingDimensions
    fun getModelDimension(dimensionName: DimensionName, modelName: ModelName): ModelDimension
    fun getModelMeasure(measureName: MeasureName, modelName: ModelName): ModelMeasure
    fun getRelation(sourceModelName: ModelName, relationName: RelationName): ModelRelation

    // Could use only dimensionName but post-operations may be used more than once
    fun getDimensionAlias(dimensionName: DimensionName, relationName: RelationName?, postOperation: ReportMetric.PostOperation?): String

    // Syntax sugar for measure. We don't need it actually.
    fun getMeasureAlias(measureName: MeasureName, relationName: String?): String
    fun getModel(modelName: ModelName): Model
    fun getAggregatesForModel(target: Model.Target, type: ReportType): List<Triple<ModelName, String, SegmentationRecipeQuery.SegmentationMaterialize>>
    fun addModel(model: Model)
    fun getSQLReference(
        modelTarget: Model.Target,
        aliasName: String,
        modelName: String,
        columnName: String?,
        inQueryDimensionNames: List<String>? = null,
        dateRange: DateRange? = null
    ): String

    fun renderSQL(
        sqlRenderable: SQLRenderable,
        alias: String?,
        modelName: ModelName?,
        inQueryDimensionNames: List<String>? = null,
        dateRange: DateRange? = null,
        // Instead of actual values, render alias
        renderAlias: Boolean = false,
        extraContext: Map<String, Any> = mapOf(),
        hook: ((Map<String, Any?>) -> Map<String, Any?>)? = null,
    ): String

    fun getUserAttributes(): UserAttributeValues {
        val fetcher = userAttributeFetcher ?: throw MetriqlException("User attribute feature is not available in the context", HttpResponseStatus.BAD_REQUEST)
        return fetcher.invoke(auth)
    }

    fun getOrGenerateAlias(modelName: String, relationName: String?): String {
        return aliases.computeIfAbsent(Pair(modelName, relationName)) {
            val size = aliases.size
            "t${size + 1}"
        }
    }
}
