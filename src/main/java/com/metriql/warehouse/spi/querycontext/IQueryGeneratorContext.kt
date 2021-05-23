package com.metriql.warehouse.spi.querycontext

import com.metriql.auth.ProjectAuth
import com.metriql.auth.UserAttributeFetcher
import com.metriql.auth.UserAttributeValues
import com.metriql.jinja.JinjaRendererService
import com.metriql.jinja.SQLRenderable
import com.metriql.model.DimensionName
import com.metriql.model.IModelService
import com.metriql.model.MeasureName
import com.metriql.model.Model
import com.metriql.model.ModelDimension
import com.metriql.model.ModelMeasure
import com.metriql.model.ModelName
import com.metriql.model.ModelRelation
import com.metriql.model.RelationName
import com.metriql.report.ReportExecutor
import com.metriql.report.ReportMetric
import com.metriql.report.ReportType
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.DataSource
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

    abstract fun getMappingDimensions(modelName: ModelName): Model.MappingDimensions
    abstract fun getModelDimension(dimensionName: DimensionName, modelName: ModelName): ModelDimension
    abstract fun getModelMeasure(measureName: MeasureName, modelName: ModelName): ModelMeasure
    abstract fun getRelation(
        sourceModelName: ModelName,
        relationName: RelationName
    ): ModelRelation

    // Could use only dimensionName but post-operations may be used more than once
    abstract fun getDimensionAlias(dimensionName: DimensionName, postOperation: ReportMetric.PostOperation?): String

    // Syntax sugar for measure. We don't need it actually.
    abstract fun getMeasureAlias(measureName: MeasureName): String
    abstract fun getModel(modelName: ModelName): Model
    abstract fun getAggregatesForModel(target: Model.Target, type: ReportType): List<Triple<ModelName, String, SegmentationRecipeQuery.SegmentationMaterialize>>
    abstract fun getAliasQuote(): Char?
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
