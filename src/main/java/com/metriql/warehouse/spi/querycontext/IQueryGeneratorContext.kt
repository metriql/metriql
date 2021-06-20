package com.metriql.warehouse.spi.querycontext

import com.metriql.report.ReportExecutor
import com.metriql.report.ReportMetric
import com.metriql.report.ReportType
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.UserAttributeFetcher
import com.metriql.service.auth.UserAttributeValues
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.DimensionName
import com.metriql.service.model.IModelService
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

abstract class IQueryGeneratorContext {
    abstract val auth: ProjectAuth
    abstract val datasource: DataSource
    abstract val reportExecutor: ReportExecutor?
    abstract val userAttributeFetcher: UserAttributeFetcher?
    abstract val modelService: IModelService
    abstract val renderer: JinjaRendererService
    abstract val columns: Set<Pair<ModelName, String>>
    abstract val dimensions: Map<Pair<ModelName, DimensionName>, ModelDimension>
    abstract val measures: Map<Pair<ModelName, MeasureName>, ModelMeasure>
    abstract val relations: Map<Pair<ModelName, RelationName>, ModelRelation>
    abstract val viewModels: LinkedHashMap<ModelName, String>
    abstract val comments: MutableList<String>
    val warehouseBridge: WarehouseMetriqlBridge get() = datasource.warehouse.bridge

    abstract fun getMappingDimensions(modelName: ModelName): Model.MappingDimensions
    abstract fun getModelDimension(dimensionName: DimensionName, modelName: ModelName): ModelDimension
    abstract fun getModelMeasure(measureName: MeasureName, modelName: ModelName): ModelMeasure
    abstract fun getRelation(
        sourceModelName: ModelName,
        relationName: RelationName
    ): ModelRelation

    // Could use only dimensionName but post-operations may be used more than once
    abstract fun getDimensionAlias(dimensionName: DimensionName, relationName: RelationName?, postOperation: ReportMetric.PostOperation?): String

    // Syntax sugar for measure. We don't need it actually.
    abstract fun getMeasureAlias(measureName: MeasureName, relationName: String?): String
    abstract fun getModel(modelName: ModelName): Model
    abstract fun getAggregatesForModel(target: Model.Target, type: ReportType): List<Triple<ModelName, String, SegmentationRecipeQuery.SegmentationMaterialize>>
    abstract fun addModel(model: Model)
    abstract fun getSQLReference(
        modelTarget: Model.Target,
        aliasName: String,
        columnName: String?,
        inQueryDimensionNames: List<String>? = null,
        dateRange: DateRange? = null
    ): String

    // Target model name only for join relations
    abstract fun renderSQL(
        sqlRenderable: SQLRenderable,
        modelName: ModelName?,
        inQueryDimensionNames: List<String>? = null,
        dateRange: DateRange? = null,
        targetModelName: ModelName? = null,
        hook: ((Map<String, Any?>) -> Map<String, Any?>)? = null,
    ): String

    fun getUserAttributes(): UserAttributeValues {
        val fetcher = userAttributeFetcher ?: throw MetriqlException("User attribute feature is not available in the context", HttpResponseStatus.BAD_REQUEST)
        return fetcher.invoke(auth)
    }
}
