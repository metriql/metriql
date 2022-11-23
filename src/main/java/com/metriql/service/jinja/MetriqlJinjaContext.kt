package com.metriql.service.jinja

import com.hubspot.jinjava.interpret.TemplateStateException
import com.metriql.report.data.FilterValue
import com.metriql.service.auth.UserAttributeValues
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.DatasetName
import com.metriql.service.dataset.ModelDimension
import com.metriql.service.dataset.ModelMeasure
import com.metriql.service.dataset.ModelRelation
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.UppercaseEnum
import com.metriql.util.serializableName
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import java.util.HashMap

sealed class MetriqlJinjaContext : HashMap<String, Any?>() {
    @UppercaseEnum
    enum class ContextType {
        PROJECTION, FILTER, // Render value of model, dimension or measure
        MODEL, DIMENSION, MEASURE, RELATION; // Initiate a new Context
    }

    class FunctionContext(private val context: IQueryGeneratorContext) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            val func = try {
                RFunction.valueOf(key.uppercase())
            } catch (e: java.lang.IllegalArgumentException) {
                throw MetriqlException("Function `$key` is not valid", BAD_REQUEST)
            }

            return context.warehouseBridge.compileFunction(func, listOf())
        }
    }

    class DecisionContext(
        private val value: Any?,
        private val context: IQueryGeneratorContext,
        private val renderAlias: Boolean,
        private val modelAlias: String?
    ) : MetriqlJinjaContext() {

        override fun toString(): String {
            return toString(WarehouseMetriqlBridge.MetricPositionType.FILTER)
        }

        fun toString(metricPositionType: WarehouseMetriqlBridge.MetricPositionType): String {
            return when (value) {
                is Dataset -> context.getSQLReference(value.target, value.name, value.name, null)
                is ModelDimension -> {
                    if (renderAlias) {
                        context.getDimensionAlias(value.dimension.name, null, null)
                    } else {
                        try {
                            "(" + context.warehouseBridge.renderDimension(
                                context,
                                value.datasetName,
                                value.dimension.name,
                                null,
                                null,
                                metricPositionType,
                                modelAlias = modelAlias
                            ).value + ")" // Joins are not possible here
                        } catch (e: MetriqlException) {
                            throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
                        }
                    }
                }
                is ModelMeasure -> {
                    if (renderAlias) {
                        context.getMeasureAlias(value.measure.name, null)
                    } else {
                        try {
                            "(" + context.warehouseBridge.renderMeasure(
                                context,
                                value.datasetName,
                                value.measure.name,
                                null,
                                metricPositionType,
                                WarehouseMetriqlBridge.AggregationContext.ADHOC,
                                value.extraFilters,
                                modelAlias = modelAlias
                            ).value + ")" // Joins are not possible here
                        } catch (e: MetriqlException) {
                            throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
                        }
                    }
                }
                is ModelRelation -> {
                    try {
                        context.warehouseBridge.generateJoinStatement(context, value)
                    } catch (e: MetriqlException) {
                        throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
                    }
                }
                else -> throw TemplateStateException("cant render a projection or filter value for context.", -1)
            }
        }

        override fun get(key: String): Any? {
            return when (val contextType = JsonHelper.convert(key, ContextType::class.java)) {
                ContextType.PROJECTION, ContextType.FILTER -> {
                    val metricPositionType = if (contextType == ContextType.FILTER) {
                        WarehouseMetriqlBridge.MetricPositionType.FILTER
                    } else {
                        WarehouseMetriqlBridge.MetricPositionType.PROJECTION
                    }
                    toString(metricPositionType)
                }
                ContextType.MODEL -> {
                    if (value !is ModelDimension || value !is ModelMeasure) {
                        throw TemplateStateException("only dimensions and measures have model", -1)
                    }
                    ModelContext(context, renderAlias, modelAlias)
                }
                ContextType.DIMENSION -> {
                    when (value) {
                        is Dataset -> DimensionContext(value.name, null, context, renderAlias, modelAlias)
                        is ModelRelation -> DimensionContext(value.targetDatasetName, value.relation, context, renderAlias, modelAlias)
                        else -> throw TemplateStateException("only models can have dimensions", -1)
                    }
                }
                ContextType.MEASURE -> {
                    when (value) {
                        is Dataset -> MeasureContext(value.name, null, context, renderAlias, modelAlias)
                        is ModelRelation -> MeasureContext(value.sourceDatasetName, value.relation, context, renderAlias, modelAlias)
                        else -> throw TemplateStateException("only models can have measures", -1)
                    }
                }
                ContextType.RELATION -> {
                    if (value is Dataset) {
                        RelationContext(value.name, context, renderAlias, modelAlias)
                    } else {
                        throw TemplateStateException("only models can have relations", -1)
                    }
                }
                else -> {
                    if (value is ModelRelation) {
                        // Fall here because it is not a context object anymore
                    }
                    throw java.lang.IllegalStateException("Unknown ContextType ${contextType.serializableName}")
                }
            }
        }
    }

    class ModelContext(
        private val context: IQueryGeneratorContext,
        private val renderAlias: Boolean,
        private val modelAlias: String?,
    ) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            val dataset: Dataset
            try {
                dataset = context.getModel(key)
            } catch (e: NoSuchElementException) {
                throw TemplateStateException(e.message, -1)
            } catch (e: MetriqlException) {
                throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
            }
            return DecisionContext(dataset, context, renderAlias, modelAlias)
        }
    }

    // Nullable model-name, if dimension context is accessed from root e.g: dimension.dimensionx.filterValue
    class RelationContext(
        private val datasetName: DatasetName?,
        private val context: IQueryGeneratorContext,
        private val renderAlias: Boolean,
        private val modelAlias: String?,
    ) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            if (datasetName == null) {
                throw TemplateStateException("missing modelName in context to render relation $key", -1)
            }
            val modelRelation: ModelRelation
            try {
                modelRelation = context.getRelation(datasetName, key)
            } catch (e: NoSuchElementException) {
                throw TemplateStateException(e.message, -1)
            } catch (e: MetriqlException) {
                throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
            }
            return DecisionContext(modelRelation, context, renderAlias, modelAlias)
        }
    }

    // Nullable model-name, if dimension context is accessed from root e.g: dimension.dimensionx.filterValue
    class DimensionContext(
        private val datasetName: DatasetName,
        private val relation: Dataset.Relation?,
        private val context: IQueryGeneratorContext,
        private val renderAlias: Boolean,
        private val modelAlias: String?
    ) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            val modelDimension = try {
                context.getModelDimension(key, relation?.datasetName ?: datasetName)
            } catch (e: NoSuchElementException) {
                throw TemplateStateException(e.message, -1)
            } catch (e: MetriqlException) {
                throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
            }

            return DecisionContext(modelDimension, context, renderAlias, modelAlias)
        }
    }

    class UserAttributeContext(context: IQueryGeneratorContext) : MetriqlJinjaContext() {
        private val map: UserAttributeValues by lazy { context.getUserAttributes() }

        override fun get(key: String): Any? {
            return map[key]?.value?.value
        }
    }

    // Nullable model-name, if measure context is accessed from root e.g: measure.name.filterValue
    class MeasureContext(
        private val datasetName: DatasetName,
        private val relation: Dataset.Relation?,
        private val context: IQueryGeneratorContext,
        private val renderAlias: Boolean,
        private val modelAlias: String?,
    ) : MetriqlJinjaContext() {
        var pushdownFilters: List<FilterValue>? = null

        override fun get(key: String): Any? {
            val modelMeasure = try {
                context.getModelMeasure(key, relation?.datasetName ?: datasetName)
            } catch (e: MetriqlException) {
                throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
            }

            return DecisionContext(modelMeasure.copy(extraFilters = pushdownFilters), context, renderAlias, modelAlias)
        }
    }

    class InQueryDimensionContext(
        private val dimensionNames: List<String>?,
    ) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            if (dimensionNames?.contains(key) == true) {
                return true
            }
            return ((dimensionNames?.count { it.startsWith(key) }) ?: 0) > 0
        }
    }
}
