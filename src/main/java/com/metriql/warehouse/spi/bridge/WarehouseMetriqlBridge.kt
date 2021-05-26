package com.metriql.warehouse.spi.bridge

import com.metriql.report.ReportFilter
import com.metriql.report.ReportMetric
import com.metriql.service.model.DimensionName
import com.metriql.service.model.MeasureName
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName
import com.metriql.service.model.ModelRelation
import com.metriql.service.model.RelationName
import com.metriql.util.MetriqlException
import com.metriql.util.serializableName
import com.metriql.warehouse.WarehouseSupports
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.filter.WarehouseFilters
import com.metriql.warehouse.spi.function.RFunction
import com.metriql.warehouse.spi.function.WarehouseTimeframes
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceQueryGenerator
import com.metriql.warehouse.spi.services.ServiceType
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import java.time.ZoneId
import kotlin.IllegalArgumentException
import kotlin.jvm.Throws

typealias RFunctions = Map<RFunction, String>

fun getRequiredFunction(functions: RFunctions, function: RFunction): String {
    return functions[function] ?: throw MetriqlException("$function function is not implemented", BAD_REQUEST)
}

interface WarehouseMetriqlBridge {
    val filters: WarehouseFilters
    val timeframes: WarehouseTimeframes
    val queryGenerators: Map<ServiceType, ServiceQueryGenerator<*, *, *>>
    val functions: RFunctions

    val supportedDBTTypes: Set<DBTType>
    val supportedJoins: Set<Model.Relation.JoinType>
    val aliasQuote: Char?
    val isCaseSensitive: Boolean get() = true

    // used for dimension projection (mainly for timezone conversion)
    val metricRenderHook: MetricRenderHook

    fun performAggregation(columnValue: String, aggregationType: Model.Measure.AggregationType, context: AggregationContext): String

    data class RenderedMetric(val metricValue: String, val join: String?)

    /**
     * In cases like rendering jinja context can't generate join expressions. Thus they're optional
     * @param contextModelName: The model context where the measured is rendered (required iff join relations are acceptable)
     * @param measureName: Name of the  measure
     * @param modelName: Model name of the measure
     * @param relationName: Join this measure using this relation name. Context model name and target required if this is set.
     * @param metricPositionType: Projection or Filter. For projection, uses alias name of the dimension (ex: (column | expression) as dimensionName)
     * @param context: An adapter class to fetch metriql model elements.
     * @return Expression and a join expression
     * */
    fun renderMeasure(
        context: IQueryGeneratorContext,
        contextModelName: ModelName,
        measureName: MeasureName,
        relationName: RelationName?,
        metricPositionType: MetricPositionType,
        queryType: AggregationContext,
        zoneId: ZoneId?,
        extraFilters: List<ReportFilter>? = null
    ): RenderedMetric

    /**
     * In cases like rendering jinja context can't generate join expressions. Thus they're optional
     * @param contextModelName: The model context where the dimension is rendered (required iff join relations are acceptable)
     * @param dimensionName: Name of the dimension
     * @param relationName: Join this dimension using this relation name. Context model name and target required if this is set.
     * @param metricPositionType: Projection or Filter. For projection, uses alias name of the dimension (ex: (column | expression) as dimensionName)
     * @param context: An adapter class to fetch metriql model elements.
     * @return Expression and a join expression
     * */
    fun renderDimension(
        context: IQueryGeneratorContext,
        contextModelName: ModelName,
        dimensionName: DimensionName,
        relationName: RelationName?,
        postOperation: ReportMetric.PostOperation?,
        metricPositionType: MetricPositionType
    ): RenderedMetric

    data class RenderedFilter(val join: String?, val whereFilter: String?, val havingFilter: String?)

    /**
     * Renders where, having and a join expression.
     * Where filters comes from the dimensions and havings from the measures.
     * If the dimension or measure has a relation to join, join expression will also be returned
     * Unlike dimension and measure rendering, context model is required in case of filtering
     * @param filter: A report filter of sql or metric
     * @param contextModelName: The model context where the filter is rendered
     * this model-name will also be passed to render dimension and measure functions.
     * @param context:  An adapter class to fetch metriql model elements.
     * @return A Where, Having and Join expression for a filter. If the filter is not applicable, IllegalArgumentException.
     * */
    @Throws(IllegalArgumentException::class)
    fun renderFilter(
        filter: ReportFilter,
        contextModelName: ModelName,
        context: IQueryGeneratorContext,
        zoneId: ZoneId?
    ): RenderedFilter

    // Generates a select query for the given dimensionNames
    fun generateDimensionMetaQuery(
        modelName: ModelName,
        modelTarget: Model.Target,
        dimensions: List<Model.Dimension>,
        context: IQueryGeneratorContext
    ): String

    /* Join-Type Join-Model ON (Join-Expression || sourceModelTarget.sourceColumn = targetModelTarget.targetColumn)
    * -> LEFT JOIN Costumer ON (Order.customerId = Costumer.id)
    * Other warehouse implementations may override this generic join statement generator
    */
    fun generateJoinStatement(
        modelRelation: ModelRelation,
        context: IQueryGeneratorContext
    ): String

    // While using a metric or dimension in query we have to include alias (name or label) for the metric
    // depending the position used in the query. This private enum class is syntactic sugar to determine whether to include alias or not.
    enum class MetricPositionType {
        PROJECTION, FILTER;
    }

    fun warehouseSupports(): WarehouseSupports {
        return WarehouseSupports(
            filters = WarehouseSupports.Filters(
                any = filters.anyOperators.keys,
                string = filters.stringOperators.keys,
                boolean = filters.booleanOperators.keys,
                number = filters.numberOperators.keys,
                timestamp = filters.timestampOperators.keys,
                date = filters.dateOperators.keys,
                time = filters.timeOperators.keys,
                array = filters.arrayOperators.keys
            ),
            postOperations = WarehouseSupports.PostOperations(
                timestamp = timeframes.timestampPostOperations.map {
                    WarehouseSupports.PostOperation(
                        it.key.serializableName,
                        it.key.valueType,
                        it.key.category
                    )
                },
                date = timeframes.datePostOperations.map {
                    WarehouseSupports.PostOperation(
                        it.key.serializableName,
                        it.key.valueType,
                        it.key.category
                    )
                },
                time = timeframes.timePostOperations.map {
                    WarehouseSupports.PostOperation(
                        it.key.serializableName,
                        it.key.valueType,
                        it.key.category
                    )
                }
            ),
            dbtTypes = supportedDBTTypes,
            aliasQuote = aliasQuote,
            services = queryGenerators.map { it.key.serializableName to it.value.supports() }.toMap(),
            aggregations = allAggregations
        )
    }

    interface MetricRenderHook {
        fun dimensionBeforePostOperation(
            context: IQueryGeneratorContext,
            metricPositionType: MetricPositionType,
            dimension: Model.Dimension,
            postOperation: ReportMetric.PostOperation?,
            dimensionValue: String
        ): String {
            return dimensionValue
        }

        fun dimensionAfterPostOperation(
            context: IQueryGeneratorContext,
            metricPositionType: MetricPositionType,
            dimension: Model.Dimension,
            postOperation: ReportMetric.PostOperation?,
            dimensionValueWithPostOperation: String
        ): String {
            return dimensionValueWithPostOperation
        }
    }

    enum class AggregationContext {
        ADHOC, INTERMEDIATE_ACCUMULATE, INTERMEDIATE_MERGE
    }

    companion object {
        val allAggregations = Model.Measure.AggregationType.values().toList()
    }

    fun generateQuery(viewModels: Map<ModelName, String>, rawQuery: String, comments: List<String> = listOf()): String
}
