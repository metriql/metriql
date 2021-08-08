package com.metriql.service.jinja

import com.hubspot.jinjava.interpret.TemplateStateException
import com.metriql.report.data.ReportFilter
import com.metriql.service.auth.UserAttributeValues
import com.metriql.service.model.Model
import com.metriql.service.model.ModelDimension
import com.metriql.service.model.ModelMeasure
import com.metriql.service.model.ModelName
import com.metriql.service.model.ModelRelation
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.UppercaseEnum
import com.metriql.util.serializableName
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import java.util.HashMap

sealed class MetriqlJinjaContext : HashMap<String, Any?>() {
    @UppercaseEnum
    enum class ContextType {
        PROJECTION, FILTER, // Render value of model, dimension or measure
        MODEL, DIMENSION, MEASURE, RELATION; // Initiate a new Context
    }

    class DecisionContext(
        private val value: Any?,
        private val context: IQueryGeneratorContext,
        private val renderAlias : Boolean
    ) : MetriqlJinjaContext() {

        override fun toString(): String {
            return toString(WarehouseMetriqlBridge.MetricPositionType.FILTER)
        }

        fun toString(metricPositionType: WarehouseMetriqlBridge.MetricPositionType): String {
            return when (value) {
                is Model -> context.getSQLReference(value.target, value.name, null)
                is ModelDimension -> {
                    if(renderAlias) {
                        context.getDimensionAlias(value.dimension.name, null, null)
                    } else {
                        try {
                            context.warehouseBridge.renderDimension(
                                context,
                                value.modelName,
                                value.dimension.name,
                                null,
                                null,
                                metricPositionType
                            ).value // Joins are not possible here
                        } catch (e: MetriqlException) {
                            throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
                        }
                    }
                }
                is ModelMeasure -> {
                    if(renderAlias) {
                        context.getMeasureAlias(value.measure.name, null)
                    } else {
                        try {
                            context.warehouseBridge.renderMeasure(
                                context,
                                value.modelName,
                                value.measure.name,
                                null,
                                metricPositionType,
                                WarehouseMetriqlBridge.AggregationContext.ADHOC,
                                value.extraFilters
                            ).value // Joins are not possible here
                        } catch (e: MetriqlException) {
                            throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
                        }
                    }
                }
                is ModelRelation -> {
                    try {
                        context.warehouseBridge.generateJoinStatement(value, context)
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
                    ModelContext(context, renderAlias)
                }
                ContextType.DIMENSION -> {
                    when (value) {
                        is Model -> DimensionContext(value.name, context, renderAlias)
                        is ModelRelation -> DimensionContext(value.targetModelName, context, renderAlias)
                        else -> throw TemplateStateException("only models can have dimensions", -1)
                    }
                }
                ContextType.MEASURE -> {
                    when (value) {
                        is Model -> MeasureContext(value.name, context, renderAlias)
                        is ModelRelation -> MeasureContext(value.targetModelName, context, renderAlias)
                        else -> throw TemplateStateException("only models can have measures", -1)
                    }
                }
                ContextType.RELATION -> {
                    if (value is Model) {
                        RelationContext(value.name, context, renderAlias)
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
        val renderAlias : Boolean
    ) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            val model: Model
            try {
                model = context.getModel(key)
            } catch (e: NoSuchElementException) {
                throw TemplateStateException(e.message, -1)
            } catch (e: MetriqlException) {
                throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
            }
            return DecisionContext(model, context, renderAlias)
        }
    }

    // Nullable model-name, if dimension context is accessed from root e.g: dimension.dimensionx.filterValue
    class RelationContext(
        private val modelName: ModelName?,
        private val context: IQueryGeneratorContext,
        private val renderAlias : Boolean
    ) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            if (modelName == null) {
                throw TemplateStateException("missing modelName in context to render relation $key", -1)
            }
            val modelRelation: ModelRelation
            try {
                modelRelation = context.getRelation(modelName, key)
            } catch (e: NoSuchElementException) {
                throw TemplateStateException(e.message, -1)
            } catch (e: MetriqlException) {
                throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
            }
            return DecisionContext(modelRelation, context, renderAlias)
        }
    }

    // Nullable model-name, if dimension context is accessed from root e.g: dimension.dimensionx.filterValue
    class DimensionContext(
        private val modelName: ModelName?,
        private val context: IQueryGeneratorContext,
        private val renderAlias : Boolean
    ) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            if (modelName == null) {
                throw TemplateStateException("missing modelName in context to render dimension $key", -1)
            }
            val modelDimension = try {
                context.getModelDimension(key, modelName)
            } catch (e: NoSuchElementException) {
                throw TemplateStateException(e.message, -1)
            } catch (e: MetriqlException) {
                throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
            }
            return DecisionContext(modelDimension, context, renderAlias)
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
        private val modelName: ModelName?,
        private val context: IQueryGeneratorContext,
        private val renderAlias : Boolean
    ) : MetriqlJinjaContext() {
        var pushdownFilters: List<ReportFilter>? = null

        override fun get(key: String): Any? {
            if (modelName == null) {
                throw TemplateStateException("missing modelName in context to render measure $key", -1)
            }

            val modelMeasure = try {
                context.getModelMeasure(key, modelName)
            } catch (e: MetriqlException) {
                throw TemplateStateException(e.errors.first().title ?: e.errors.first().detail, -1)
            }

            return DecisionContext(modelMeasure.copy(extraFilters = pushdownFilters), context, renderAlias)
        }
    }

    class InQueryDimensionContext(
        private val dimensionNames: List<String>?,
    ) : MetriqlJinjaContext() {
        override fun get(key: String): Any? {
            if (dimensionNames?.contains(key) == true) {
                return true
            }
            return (dimensionNames?.filter { it.startsWith(key) }?.count()) ?: 0 > 0
        }
    }
}
