package com.metriql.report.segmentation

import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.TIME_SERIES
import com.metriql.service.dataset.Dataset.Target.TargetValue.Table
import com.metriql.service.dataset.DatasetName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.DbtSettings.Companion.generateSchemaForModel
import com.metriql.warehouse.spi.function.IPostOperation
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.MaterializeQuery.Companion.defaultModelName
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND

typealias ErrorMessage = String

class SegmentationQueryReWriter(val context: IQueryGeneratorContext) {
    data class MaterializeTableCache(val projectId: String, val target: Table)

    fun findOptimumPlan(
        query: SegmentationQuery,
        aggregates: List<Triple<DatasetName, String, SegmentationMaterialize>>,
        caches: ((MaterializeTableCache) -> Boolean)? = null,
    ): Pair<SegmentationQuery, Dataset>? {
        val plans = aggregates.mapNotNull {
            val materializeModelName = it.third.getModelName() ?: defaultModelName(it.first, SegmentationReportType, it.second)
            val model = getModel(materializeModelName, it.third, query)

            when (val planOrErrorMessage = generateQueryIfPossible(query, model, it.third, materializeModelName)) {
                is Either.Right -> {
                    context.comments.add("Unable to use materialize [${it.first}, {${it.second}}]: ${planOrErrorMessage.value}")
                    null
                }
                is Either.Left -> {
                    val cacheKey = MaterializeTableCache(context.auth.projectId, model.target.value as Table)
                    when {
                        caches == null || caches.invoke(cacheKey) -> Pair(planOrErrorMessage.value, model)
                        else -> {
                            context.comments.add("Unable to use materialize [${it.first}]: The target table ${model.target.value} doesn't exist")
                            null
                        }
                    }
                }
            }
        }

        return plans.firstOrNull()
    }

    private fun generateQueryIfPossible(
        query: SegmentationQuery,
        materializeDataset: Dataset,
        materializeQuery: SegmentationMaterialize,
        materializeName: String,
    ): Either<SegmentationQuery, ErrorMessage> {
        return Either.Right("Feature is disabled")

//        val sourceModel = context.getModel(query.modelName)
//
//        query.measures.forEach { measure ->
//            val reference = measure.toMetricReference()
//            if (!materializeQuery.measures.contains(reference)) {
//                if (measure.relationName == null || !materializeModel.relations.any { it.name == measure.relationName }) {
//                    return Either.Right("Measure does not exist: ${encode(reference)}")
//                }
//            }
//        }
//
//        val aggregateFilters = (materializeQuery.filters ?: listOf()).toMutableList()
//        val newFilters = mutableListOf<ReportFilter>()
//
//        for (filter in query.filters) {
//            val fittedFilters = aggregateFilters.map { it.toReportFilter(context, query.modelName).value.subtract(filter) }
//
//            val fullMatchIndex = fittedFilters.indexOfFirst { it == null }
//            if (fullMatchIndex > -1) {
//                aggregateFilters.removeAt(fullMatchIndex)
//                continue
//            }
//
//            val partialMatchIndex = fittedFilters.indexOfFirst { it?.value != filter.value }
//            if (partialMatchIndex > -1) {
//                aggregateFilters.removeAt(partialMatchIndex)
//                newFilters.add(fittedFilters[partialMatchIndex]!!)
//                continue
//            }
//
//            when (filter.value) {
//                is ReportFilter.FilterValue.MetricFilter -> {
//                    filter.value.filters.forEach {
//                        when (val metricValue = it.metricValue) {
//                            is ReportMetric.ReportDimension -> {
//                                try {
//                                    newFilters.add(getFilterForDimension(materializeQuery, metricValue, filter.value.filters))
//                                } catch (e: IllegalArgumentException) {
//                                    return Either.Right(e.message) as Either<SegmentationReportOptions, ErrorMessage>
//                                }
//                            }
//                            is ReportMetric.ReportMeasure -> {
//                                val materializeIncludesMeasure = materializeQuery.measures.contains(metricValue.toMetricReference())
//                                if (materializeIncludesMeasure) {
//                                    newFilters.add(
//                                        ReportFilter(
//                                            METRIC,
//                                            ReportFilter.FilterValue.MetricFilter(AND, filter.value.filters.map { it.copy(metricType = MEASURE, metricValue = metricValue) })
//                                        )
//                                    )
//                                } else {
//                                    return Either.Right("Materialize doesn't include query measure ${encode(metricValue.toMetricReference())}")
//                                }
//                            }
//                            is ReportMetric.ReportMappingDimension -> {
//                                val dimensionName = sourceModel.mappings.get(metricValue.name) ?: throw IllegalArgumentException("")
//                                val dimension = ReportMetric.ReportDimension(dimensionName, materializeModel.name, null, metricValue.postOperation)
//                                try {
//                                    newFilters.add(getFilterForDimension(materializeQuery, dimension, filter.value.filters))
//                                } catch (e: IllegalArgumentException) {
//                                    return Either.Right(e.message) as Either<SegmentationReportOptions, ErrorMessage>
//                                }
//                            }
//                            else -> throw IllegalStateException()
//                        }
//                    }
//                }
//                else -> return Either.Right("${filter.type} filters are not supported.")
//            }
//        }
//
//        if (aggregateFilters.isNotEmpty()) {
//            return Either.Right("Materialized query is a subset of this query: ${encode(aggregateFilters)}")
//        }
//
//        val dimensions = query.dimensions?.map { dimension ->
//            val dimReference = dimension.toReference()
//            (materializeQuery.dimensions ?: listOf()).mapNotNull { materializeDimRef ->
//                val type = materializeDimRef.getType(context, sourceModel.name)
//                val materializeDimension = materializeDimRef.toDimension(materializeName, type)
//
//                if (materializeDimRef.name != dimReference.name) {
//                    null
//                } else if (materializeDimRef.timeframe != null || dimReference.timeframe != null) {
//                    when (val postOperation = dimension.postOperation?.value) {
//                        is IPostOperation -> {
//                            when {
//                                postOperation == materializeDimension.postOperation?.value -> {
//                                    materializeDimension.copy(postOperation = null)
//                                }
//                                materializeDimension.postOperation == null || (materializeDimension.postOperation.value as IPostOperation).isInclusive(postOperation) -> {
//                                    materializeDimension.copy(postOperation = dimension.postOperation)
//                                }
//                                else -> {
//                                    null
//                                }
//                            }
//                        }
//                        else -> throw IllegalStateException()
//                    }
//                } else {
//                    materializeDimension.copy(relationName = null)
//                }
//            }.firstOrNull() ?: return Either.Right("`${encode(dimReference)}` dimension doesn't fit in.")
//        }
//
//        return Either.Left(
//            query.copy(
//                modelName = materializeName,
//                measures = query.measures,
//                dimensions = dimensions,
//                filters = newFilters
//            )
//        )
    }

//    private fun getFilterForDimension(materializeQuery: SegmentationMaterialize, metricValue: ReportMetric.ReportDimension, filters: List<Filter>): ReportFilter {
//        val reference = metricValue.toReference()
//        if (materializeQuery.dimensions?.any { it.name == reference.name } == true) {
//            return ReportFilter(
//                METRIC,
//                ReportFilter.FilterValue.MetricFilter(
//                    AND, filters.map { it.copy(DIMENSION, metricValue.copy(relation = null)) }
//                )
//            )
//        } else {
//            throw IllegalArgumentException("Materialize doesn't include query dimension ${encode(reference)}")
//        }
//    }

    private fun getModel(modelName: String, materializeQuery: SegmentationMaterialize, query: SegmentationQuery): Dataset {
        val sourceModel = context.getModel(query.dataset)

        val reportOptions = materializeQuery.toQuery(modelName) as SegmentationQuery

        val dimensions = materializeQuery.dimensions?.map {
            val type = it.getType(context, sourceModel.name).second
            val dimension = it.toDimension(sourceModel.name, type)
            Dataset.Dimension(
                it.name,
                Dataset.Dimension.Type.COLUMN,
                Dataset.Dimension.DimensionValue.Column(context.getDimensionAlias(it.name, it.relation, dimension.timeframe)),
                fieldType = if (dimension.timeframe != null) {
                    (dimension.timeframe.value as IPostOperation).valueType
                } else {
                    type
                }
            )
        } ?: listOf()

        val measures = reportOptions.measures?.map { it ->
            val measure = sourceModel.measures.find { m -> m.name == it.name } ?: throw MetriqlException("Measure ${it.name} in ${sourceModel.name} not found", NOT_FOUND)
            val aggregation = measure.value.agg ?: throw MetriqlException("`aggregation` is required for intermediate state of the measure ${it.name}", BAD_REQUEST)
            Dataset.Measure(
                it.name,
                null,
                null,
                null,
                Dataset.Measure.Type.COLUMN,
                Dataset.Measure.MeasureValue.Column(aggregation, context.getMeasureAlias(it.name, it.relation))
            )
        }

        val relations = sourceModel.relations.filter { relation ->
            when (relation.value) {
                is Dataset.Relation.RelationValue.DimensionValue -> {
                    dimensions.any { it.name == relation.value.sourceDimension }
                }
                else -> false
            }
        }

        val dependency = context.getDependencies(sourceModel.name)
        val schema = generateSchemaForModel(context.datasource.config.warehouseSchema(), dependency.dbtDependency().aggregateSchema())
        val target = Table(context.datasource.config.warehouseDatabase(), schema, modelName)
        return Dataset(
            modelName,
            false,
            Dataset.Target(Dataset.Target.Type.TABLE, target),
            null,
            null,
            null,
            Dataset.MappingDimensions.build(TIME_SERIES to sourceModel.mappings.get(TIME_SERIES)),
            relations,
            dimensions,
            measures ?: listOf()
        )
    }

    sealed class Either<out A, out B> {
        class Left<A>(val value: A) : Either<A, Nothing>()
        class Right<B>(val value: B) : Either<Nothing, B>()
    }
}
