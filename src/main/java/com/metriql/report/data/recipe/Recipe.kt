package com.metriql.report.data.recipe

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver
import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.db.FieldType
import com.metriql.db.JSONBSerializable
import com.metriql.dbt.DbtJinjaRenderer
import com.metriql.dbt.DbtManifest
import com.metriql.dbt.DbtManifest.Companion.extractFields
import com.metriql.report.ReportType
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.DimensionName
import com.metriql.service.model.Model
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP
import com.metriql.service.model.ModelName
import com.metriql.service.model.RelationName
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.UppercaseEnum
import com.metriql.util.getOperation
import com.metriql.util.serializableName
import com.metriql.util.toSnakeCase
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation.HOUR
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND

data class Recipe(
    val repository: String,
    val branch: String,
    val path: String? = null,
    val config: Config,
    val packageName: String?,
    val dependencies: Dependencies? = null,
    val models: List<RecipeModel>? = null,
    val dashboards: List<RecipeDashboard>? = null,
    val reports: List<SaveReport>? = null,
) {
    @JsonIgnoreProperties
    fun getDependenciesWithFallback(): Dependencies {
        return dependencies ?: Dependencies()
    }

    @JsonIgnoreProperties(value = ["dbtDependency"])
    @JSONBSerializable
    data class Dependencies(val dbt: DbtDependency? = null) {
        @JsonIgnoreProperties(value = ["packages", "dbtProject", "selfHosted", "cronjob", "target"])
        data class DbtDependency(
            val profile: String? = null,
            val aggregatesSchema: String? = null,
            val aggregatesDirectory: String? = null,
        ) {
            @JsonIgnore
            fun aggregateSchema(): String {
                return aggregatesSchema ?: "metriql_aggregates"
            }

            @JsonIgnore
            fun aggregatesDirectory(): String {
                return aggregatesSchema ?: "models/metriql"
            }

            @JsonIgnore
            fun profiles(): String {
                return profile ?: "default"
            }
        }

        fun dbtDependency(): DbtDependency {
            return dbt ?: DbtDependency()
        }
    }

    @JsonIgnoreProperties(value = ["version"])
    data class Config(
        val label: String,
        val description: String? = null,
        val image: String? = null,
        val tags: List<String>? = null,
        val variables: Map<String, RecipeVariable>? = null,
        val databases: List<String>? = null,
    ) {
        data class RecipeVariable(
            // do not show the variable if the user doesn't have write access to GIT repo
            val git: Boolean?,
            val type: String,
            val hidden: Boolean?,
            val required: Boolean?,
            val label: String?,
            val parent: String?,
            val description: String?,
            val default: Any?,
            val options: ObjectNode?,
        )
    }

    data class RecipeModel(
        val name: String? = null,
        val hidden: Boolean? = null,
        val target: Model.Target.TargetValue.Table? = null,
        val sql: SQLRenderable? = null,
        val dataset: String? = null,
        val label: String? = null,
        val description: String? = null,
        val category: String? = null,
        val extends: String? = null,
        val mappings: Model.MappingDimensions? = null,
        val relations: Map<String, RecipeRelation>? = null,
        val dimensions: Map<String, Metric.RecipeDimension>? = null,
        val columns: List<DbtManifest.DbtColumn>? = null,
        val measures: Map<String, Metric.RecipeMeasure>? = null,
        val materializes: Map<String, Map<String, SegmentationRecipeQuery.SegmentationMaterialize>>? = null,
        val aggregates: Map<String, SegmentationRecipeQuery.SegmentationMaterialize>? = null,
        val tags: List<String>? = null,
        @JsonAlias("always_filters")
        val alwaysFilters: List<OrFilters>? = null,
        @JsonIgnore
        val _path: String? = null,
        val package_name: String? = null,
    ) {

        @JsonIgnore
        fun getMaterializes(): List<Model.Materialize> {
            val aggregates = this.aggregates?.let { mapOf(SegmentationReportType to it) } ?: mapOf()
            val allMaterializes = ((materializes ?: mapOf()) + aggregates).flatMap { it.value.entries }?.map {
                Model.Materialize(
                    it.key,
                    SegmentationReportType,
                    it.value
                )
            }

            allMaterializes.forEach { materialize ->
                when (materialize.reportType) {
                    SegmentationReportType -> {
                        val eventTimestamp = mappings?.get(EVENT_TIMESTAMP)
                        if (eventTimestamp != null) {
                            val hasEventTimestampDimension = materialize.value.dimensions?.any {
                                // TODO: if the dimension type is timestamp
                                if (false) {
                                    val timeframe = it.timeframe ?: throw MetriqlException(
                                        "Timeframe is required for ${it.name} dimension",
                                        BAD_REQUEST
                                    )
                                    val enum = JsonHelper.convert(timeframe, TimestampPostOperation::class.java)
                                    if (enum != HOUR && !enum.isInclusive(TimestampPostOperation.YEAR)) {
                                        throw MetriqlException(
                                            "One of HOUR, DAY, WEEK, MONTH or YEAR timeframe of the eventTimestamp is required for incremental models",
                                            BAD_REQUEST
                                        )
                                    }

                                    true
                                } else {
                                    false
                                }
                            } ?: false
                        }
                    }
                    else -> throw MetriqlException(
                        "Only segmentation materializes are allowed at the moment.",
                        HttpResponseStatus.NOT_IMPLEMENTED
                    )
                }
            }

            return allMaterializes
        }

        @JsonIgnore
        fun validate(): List<String> {
            val errors = mutableListOf<String>()

            if (name == null) {
                errors.add("($name): `name` is required to define a model")
            }

            if (sql == null && target == null) {
                errors.add("($name): Either `sql` or `target` is required to define a model")
            }

            if (name != null) {
                dimensions?.forEach {
                    it.value.validate(name, it.key)?.let { err -> errors.add(err) }
                }

                measures?.forEach {
                    it.value.validate(name, it.key)?.let { err -> errors.add(err) }
                }
            }

            return errors
        }

        companion object {
            fun fromDimension(dimension: Model.Dimension): Metric.RecipeDimension {
                return when (dimension.value) {
                    is Model.Dimension.DimensionValue.Sql -> Metric.RecipeDimension(
                        dimension.label,
                        dimension.description,
                        dimension.category,
                        null,
                        dimension.postOperations,
                        dimension.fieldType ?: FieldType.UNKNOWN,
                        null,
                        dimension.value.sql,
                        dimension.reportOptions,
                        null,
                        dimension.primary,
                        dimension.value.window
                    )
                    is Model.Dimension.DimensionValue.Column -> Metric.RecipeDimension(
                        dimension.label,
                        dimension.description,
                        dimension.category,
                        null,
                        dimension.postOperations,
                        dimension.fieldType ?: FieldType.UNKNOWN,
                        dimension.value.column,
                        null,
                        dimension.reportOptions,
                        null,
                        dimension.primary,
                        null,
                        dimension.tags
                    )
                }
            }

            fun fromModel(model: Model): RecipeModel {
                val recipeRelations = model.relations.map { relation ->
                    relation.name to when (relation.value) {
                        is Model.Relation.RelationValue.SqlValue -> RecipeRelation(
                            relation.label,
                            relation.description,
                            relation.relationType,
                            relation.joinType,
                            relation.modelName,
                            relation.value.sql,
                            null,
                            null,
                            null,
                            null,
                            if (relation.hidden === true) true else null,
                        )
                        is Model.Relation.RelationValue.ColumnValue -> RecipeRelation(
                            relation.label,
                            relation.description,
                            relation.relationType,
                            relation.joinType,
                            relation.modelName,
                            null,
                            null,
                            null,
                            relation.value.sourceColumn,
                            relation.value.targetColumn,
                            if (relation.hidden === true) true else null
                        )
                        is Model.Relation.RelationValue.DimensionValue -> RecipeRelation(
                            relation.label,
                            relation.description,
                            relation.relationType,
                            relation.joinType,
                            relation.modelName,
                            null,
                            relation.value.sourceDimension,
                            relation.value.targetDimension,
                            null,
                            null,
                            if (relation.hidden === true) true else null
                        )
                    }
                }.toMap()

                val recipeDimensions = model.dimensions.map { dimension ->
                    dimension.name to fromDimension(dimension)
                }.toMap()

                val recipeMeasures = model.measures.map { measure ->
                    val filters = if (measure.filters != null) {
                        measure.filters
                            .map { filter ->
                                when (filter.value) {
                                    is ReportFilter.FilterValue.Sql -> {
                                        listOf(
                                            Metric.RecipeMeasure.Filter(
                                                null,
                                                null,
                                                filter.value.sql,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null
                                            )
                                        )
                                    }
                                    is ReportFilter.FilterValue.MetricFilter -> {
                                        filter.value.filters.map {
                                            val metricValue = filter.value.metricValue ?: it.metricValue ?: throw java.lang.IllegalArgumentException()
                                            when (metricValue) {
                                                is ReportMetric.ReportMeasure -> throw IllegalStateException("Measure filters can't filter measures")
                                                is ReportMetric.ReportDimension -> {
                                                    val dimensionType = model.dimensions.find { dim -> dim.name == metricValue.name }?.fieldType
                                                        ?: throw MetriqlException(
                                                            "Dimension ${model.name}.${metricValue.name} must have type since these is filter set on it.",
                                                            NOT_FOUND
                                                        )
                                                    val (type, operation) = getOperation(dimensionType, it.operator)
                                                    filter.value.filters
                                                        .map {
                                                            Metric.RecipeMeasure.Filter(
                                                                metricValue.name,
                                                                null,
                                                                null,
                                                                metricValue.modelName,
                                                                metricValue.relationName,
                                                                metricValue.postOperation,
                                                                type,
                                                                operation,
                                                                it.value
                                                            )
                                                        }
                                                }
                                                is ReportMetric.ReportMappingDimension -> {
                                                    val name = model.mappings.get(metricValue.name)
                                                    val dimensionType = model.dimensions.find { dim -> dim.name == name }?.fieldType
                                                        ?: throw MetriqlException(
                                                            "Dimension ${model.name}.${metricValue.name} must have type since these is filter set on it.",
                                                            NOT_FOUND
                                                        )
                                                    val (type, operator) = getOperation(dimensionType, it.operator)

                                                    filter.value.filters
                                                        .map {
                                                            Metric.RecipeMeasure.Filter(
                                                                null,
                                                                metricValue.name,
                                                                null,
                                                                null,
                                                                null,
                                                                metricValue.postOperation,
                                                                type,
                                                                operator,
                                                                it.value
                                                            )
                                                        }
                                                }
                                                is ReportMetric.Function -> TODO()
                                                is ReportMetric.Unary -> TODO()
                                                else -> TODO()
                                            }
                                        }.flatten()
                                    }
                                }
                            }.flatten()
                    } else {
                        null
                    }
                    measure.name to when (measure.value) {
                        is Model.Measure.MeasureValue.Sql -> {
                            Metric.RecipeMeasure(
                                measure.label,
                                measure.description,
                                measure.category,
                                filters,
                                measure.reportOptions,
                                measure.value.sql,
                                null,
                                null,
                                measure.value.aggregation,
                                measure.fieldType,
                                if (measure.hidden === true) true else null,
                                measure.value.window
                            )
                        }
                        is Model.Measure.MeasureValue.Column -> {
                            Metric.RecipeMeasure(
                                measure.label,
                                measure.description,
                                measure.category,
                                filters,
                                measure.reportOptions,
                                null,
                                measure.value.column,
                                null,
                                measure.value.aggregation,
                                measure.fieldType,
                                if (measure.hidden === true) true else null,
                                null
                            )
                        }
                        is Model.Measure.MeasureValue.Dimension -> {
                            Metric.RecipeMeasure(
                                measure.label,
                                measure.description,
                                measure.category,
                                filters,
                                measure.reportOptions,
                                null,
                                null,
                                measure.value.dimension,
                                measure.value.aggregation,
                                measure.fieldType,
                                if (measure.hidden === true) true else null,
                                null
                            )
                        }
                    }
                }.toMap()

                val aggregates = model.materializes?.filter { it.reportType == SegmentationReportType }?.associate { it.name to it.value }
                return RecipeModel(
                    model.name,
                    if (model.hidden === true) true else null,
                    model.target.value as? Model.Target.TargetValue.Table,
                    (model.target.value as? Model.Target.TargetValue.Sql)?.sql,
                    null,
                    model.label,
                    model.description,
                    model.category,
                    null,
                    model.mappings,
                    recipeRelations,
                    recipeDimensions,
                    null,
                    recipeMeasures,
                    null,
                    aggregates,
                    model.tags,
                    model.alwaysFilters,
                    model.recipePath
                )
            }
        }

        fun toModel(packageNameFromModel: String, bridge: WarehouseMetriqlBridge, recipeId: Int): Model {
            val packageName = this.package_name ?: packageNameFromModel
            val modelName = name ?: throw MetriqlException("Model name is required", BAD_REQUEST)

            val modelRelations = relations?.map { (relationName, relation) -> relation.toRelation(packageName, relationName) } ?: listOf()

            val modelTarget = when {
                sql != null -> {
                    Model.Target(Model.Target.Type.SQL, Model.Target.TargetValue.Sql(sql))
                }
                dataset != null -> {
                    val modelName = DbtJinjaRenderer.renderer.renderReference(dataset, packageName)
                    Model.Target(Model.Target.Type.SQL, Model.Target.TargetValue.Sql("select * from {{model.$modelName}}"))
                }
                else -> {
                    Model.Target(Model.Target.Type.TABLE, target!!)
                }
            }

            val (dbtMeasures, dbtDimensions) = extractFields(modelName, columns?.map { it.name to it }?.toMap() ?: mapOf())

            return Model(
                modelName,
                hidden ?: false,
                modelTarget,
                label,
                description,
                category,
                mappings ?: Model.MappingDimensions(),
                modelRelations,
                ((dimensions ?: mapOf()) + dbtDimensions)?.map { (dimensionName, dimension) -> dimension.toDimension(dimensionName, bridge) },
                ((measures ?: mapOf()) + dbtMeasures)?.map { (measureName, measure) -> measure.toMeasure(measureName, modelName) },
                getMaterializes(),
                alwaysFilters,
                null,
                tags,
                recipeId,
                _path
            )
        }

        sealed class Metric {
            data class RecipeDimension(
                val label: String? = null,
                val description: String? = null,
                val category: String? = null,
                val pivot: Boolean? = null,
                val timeframes: List<String>? = null,
                val type: FieldType? = null,
                val column: String? = null,
                val sql: SQLRenderable? = null,
                @JsonAlias("report")
                val reportOptions: ObjectNode? = null,
                val hidden: Boolean? = null,
                val primary: Boolean? = null,
                val window: Boolean? = null,
                val tags: List<String>? = null,
                val name: String? = null,
            ) : Metric() {

                @JsonIgnore
                fun validate(modelName: String, name: String?): String? {
                    if (sql == null && column == null) {
                        return "$modelName.${name ?: this.name}: One of 'sql' and 'column' is required in dimension"
                    }

                    if (sql != null && column != null) {
                        return "$modelName.${name ?: this.name}: Only one of 'sql' and 'column' is required in dimension"
                    }

                    return null
                }

                fun toDimension(dimensionName: String, bridge: WarehouseMetriqlBridge): Model.Dimension {
                    val (valType, value) = if (sql != null) {
                        Pair(Model.Dimension.Type.SQL, Model.Dimension.DimensionValue.Sql(sql, window))
                    } else {
                        Pair(Model.Dimension.Type.COLUMN, Model.Dimension.DimensionValue.Column(column!!))
                    }

                    val timeframes = if (timeframes.isNullOrEmpty() && type != null) {
                        when (type) {
                            FieldType.TIMESTAMP -> bridge.timeframes.timestampPostOperations.map { it.key.serializableName }
                            FieldType.DATE -> bridge.timeframes.datePostOperations.map { it.key.serializableName }
                            FieldType.TIME -> bridge.timeframes.timePostOperations.map { it.key.serializableName }
                            else -> null
                        }
                    } else {
                        timeframes
                    }

                    return Model.Dimension(
                        dimensionName,
                        valType,
                        value,
                        description,
                        label,
                        category,
                        primary,
                        if (pivot === false) false else null,
                        null,
                        timeframes,
                        type,
                        reportOptions,
                        if (hidden === true) true else null
                    )
                }
            }

            data class RecipeMeasure(
                val label: String? = null,
                val description: String? = null,
                val category: String? = null,
                val filters: List<Filter>? = null,
                @JsonAlias("report")
                val reportOptions: ObjectNode? = null,
                val sql: SQLRenderable? = null,
                val column: String? = null,
                val dimension: String? = null,
                val aggregation: Model.Measure.AggregationType? = null,
                val type: FieldType? = FieldType.DOUBLE,
                val hidden: Boolean? = null,
                val window: Boolean? = null,
                val tags: List<String>? = null,
                val name: String? = null,
            ) : Metric() {

                fun validate(modelName: String, name: String?): String? {
                    if (sql == null && (column == null && aggregation == null)) {
                        return "$modelName.${name ?: this.name}: 'column' or 'sql' should be defined for a measure"
                    }
                    if (column != null && sql != null) {
                        return "$modelName.${name ?: this.name}: `sql` is not allowed for column definitions."
                    }
                    if (column != null && aggregation == null) {
                        return "$modelName.${name ?: this.name}: Measure aggregation is required when column is present"
                    }

                    return null
                }

                fun toMeasure(measureName: String, modelName: String): Model.Measure {
                    val (valType, value) = when {
                        sql != null -> {
                            Pair(Model.Measure.Type.SQL, Model.Measure.MeasureValue.Sql(sql, aggregation, window))
                        }
                        dimension != null -> {
                            val agg = aggregation ?: throw MetriqlException("Aggregation is required for $modelName.$measureName", BAD_REQUEST)
                            Pair(
                                Model.Measure.Type.DIMENSION,
                                Model.Measure.MeasureValue.Dimension(agg, dimension)
                            )
                        }
                        else -> {
                            val agg = aggregation ?: throw MetriqlException("Aggregation is required for $modelName.$measureName", BAD_REQUEST)
                            Pair(
                                Model.Measure.Type.COLUMN,
                                Model.Measure.MeasureValue.Column(agg, column)
                            )
                        }
                    }

                    val filters = filters?.map { filter ->
                        when {
                            filter.dimension != null -> {
                                ReportFilter(
                                    ReportFilter.Type.METRIC_FILTER,
                                    ReportFilter.FilterValue.MetricFilter(
                                        // TODO: it's deprecated
                                        ReportFilter.FilterValue.MetricFilter.MetricType.DIMENSION,
                                        // TODO: it's deprecated
                                        ReportMetric.ReportDimension(
                                            filter.dimension,
                                            filter.modelName ?: modelName,
                                            filter.relationName,
                                            filter.postOperation
                                        ),
                                        listOf(ReportFilter.FilterValue.MetricFilter.Filter(null, null, filter.operator!!.name, filter.value))
                                    )
                                )
                            }
                            filter.mappingDimension != null -> {
                                ReportFilter(
                                    ReportFilter.Type.METRIC_FILTER,
                                    ReportFilter.FilterValue.MetricFilter(
                                        ReportFilter.FilterValue.MetricFilter.MetricType.MAPPING_DIMENSION,
                                        ReportMetric.ReportMappingDimension(
                                            filter.mappingDimension,
                                            filter.postOperation
                                        ),
                                        listOf(ReportFilter.FilterValue.MetricFilter.Filter(null, null, filter.operator!!.name, filter.value))
                                    )
                                )
                            }
                            filter.sql != null -> {
                                ReportFilter(
                                    ReportFilter.Type.SQL_FILTER,
                                    ReportFilter.FilterValue.Sql(filter.sql)
                                )
                            }
                            else -> throw IllegalStateException("dimension, mappingDimension or sql must be present in a measure filter")
                        }
                    }
                    return Model.Measure(
                        measureName,
                        label,
                        description,
                        category,
                        valType,
                        value,
                        filters,
                        reportOptions,
                        type,
                        if (hidden === true) true else null,
                        tags
                    )
                }

                data class Filter(
                    val dimension: DimensionName? = null,
                    @JsonAlias("mapping")
                    val mappingDimension: Model.MappingDimensions.CommonMappings? = null,
                    val sql: SQLRenderable? = null,
                    val modelName: ModelName? = null,
                    val relationName: RelationName? = null,
                    val postOperation: ReportMetric.PostOperation? = null,
                    val valueType: FieldType? = null,
                    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "valueType", include = JsonTypeInfo.As.EXTERNAL_PROPERTY)
                    @JsonTypeIdResolver(ReportFilter.FilterValue.MetricFilter.Filter.OperatorTypeResolver::class)
                    val operator: Enum<*>? = null,
                    val value: Any? = null,
                ) {
                    init {
                        if (dimension == null && mappingDimension == null && sql == null) {
                            throw MetriqlException(
                                "sql, dimension or mappingDimension must be present in a measure filter",
                                BAD_REQUEST
                            )
                        }
                        if (dimension != null && mappingDimension != null && sql != null) {
                            throw MetriqlException(
                                "only sql, dimension or mappingDimension must be present in a measure filter",
                                BAD_REQUEST
                            )
                        }
                        if (sql == null && operator == null) {
                            throw MetriqlException(
                                "operator is required when dimension or mappingDimension is set",
                                BAD_REQUEST
                            )
                        }
                    }
                }
            }
        }

        data class RecipeRelation(
            val label: String? = null,
            val description: String? = null,
            @JsonAlias("relationType", "relation")
            val relationship: Model.Relation.RelationType? = null,
            @JsonAlias("join", "joinType")
            val type: Model.Relation.JoinType? = null,
            @JsonAlias("modelName")
            val model: ModelName? = null,
            val sql: SQLRenderable? = null,
            @JsonAlias("sourceDimension")
            val source: String? = null,
            @JsonAlias("targetDimension")
            val target: String? = null,
            val sourceColumn: String? = null,
            val targetColumn: String? = null,
            val hidden: Boolean? = null,
            val to: String? = null,
            val fields: Set<String>? = null,
            val name: String? = null
        ) {
            fun getModel(packageName: String, relationName: String): ModelName {
                val toModel = to?.let { DbtJinjaRenderer.renderer.renderReference("{{$it}}", packageName) }
                return model ?: toModel ?: relationName
            }

            fun toRelation(packageName: String, relationName: String): Model.Relation {
                val (relType, value) = when {
                    sql != null -> {
                        Pair(Model.Relation.Type.SQL, Model.Relation.RelationValue.SqlValue(sql))
                    }
                    source != null -> {
                        Pair(
                            Model.Relation.Type.DIMENSION,
                            Model.Relation.RelationValue.DimensionValue(source, target!!)
                        )
                    }
                    else -> {
                        Pair(
                            Model.Relation.Type.COLUMN,
                            Model.Relation.RelationValue.ColumnValue(sourceColumn!!, targetColumn!!)
                        )
                    }
                }

                return Model.Relation(
                    relationName,
                    label,
                    description,
                    relationship ?: Model.Relation.RelationType.ONE_TO_ONE,
                    type ?: Model.Relation.JoinType.LEFT_JOIN,
                    getModel(packageName, relationName),
                    relType,
                    value,
                    hidden,
                    fields
                )
            }
        }
    }

    data class FilterReference(
        val dimension: FieldReference? = null,
        val measure: FieldReference? = null,
        val mapping: String? = null,
        val operator: String,
        val value: Any?,
    )

    data class FieldReference(val name: String, val relation: String? = null, val timeframe: String? = null) {
        @JsonValue
        override fun toString(): String {
            val ref = if (relation != null) "$relation.$name" else name
            return if (timeframe != null) "$ref::$timeframe" else ref
        }

        fun getType(context: IQueryGeneratorContext, modelName: ModelName): FieldType {
            val currentModel = context.getModel(modelName)

            val targetModel = if (relation != null) {
                val targetModelName = currentModel.relations.find { r -> r.name == relation }?.modelName
                context.getModel(targetModelName!!)
            } else currentModel

            val dimensionName = if (isMappingDimension()) {
                targetModel.mappings[name.substring(1)]
            } else {
                name
            }

            val measureModel = if (relation != null) {
                context.getModel(currentModel.relations.find { it.name == relation }?.modelName!!)
            } else currentModel

            val measureType = measureModel.measures.find { it.name == name }?.fieldType

            val dimensionType =
                targetModel.dimensions.find { dimension -> dimension.name == dimensionName }?.fieldType

            return measureType ?: dimensionType ?: FieldType.UNKNOWN
        }

        @JsonIgnore
        fun isMappingDimension() = name.startsWith(":")

        fun toMeasure(modelName: ModelName): ReportMetric.ReportMeasure {
            if (timeframe != null) {
                throw MetriqlException("`$this`: timeframe can't be used for measure references", BAD_REQUEST)
            }
            return ReportMetric.ReportMeasure(modelName, name, relation)
        }

        @JsonIgnore
        fun toDimension(modelName: ModelName, type: FieldType): ReportMetric.ReportDimension {
            return ReportMetric.ReportDimension(
                name, modelName, relation,
                if (timeframe != null)
                    try {
                        ReportMetric.PostOperation.fromFieldType(type, timeframe)
                    } catch (e: IllegalArgumentException) {
                        throw MetriqlException("Error constructing dimension $this: ${e.message}", BAD_REQUEST)
                    }
                else null,
                null
            )
        }

        companion object {
            private val regex = "(([A-Z0-9a-z_]+)?\\.)?(\\$?[:A-Z0-9a-z_]+)".toRegex()

            fun mappingDimension(name: Model.MappingDimensions.CommonMappings, relation: String?): FieldReference {
                return FieldReference(":" + name.toSnakeCase, relation)
            }

            @JvmStatic
            @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
            fun fromName(name: String): FieldReference {
                val split = name.split("::")
                val groups = regex.find(split[0])?.groupValues ?: throw MetriqlException(
                    "Invalid value $name for metric",
                    BAD_REQUEST
                )
                return FieldReference(groups[3], groups[2].ifEmpty { null }, split.getOrNull(1))
            }
        }
    }

    @UppercaseEnum
    enum class OrderType {
        ASC, DESC
    }

    data class SaveReport(
        val name: String,
        val description: String?,
        val category: String?,
        val sharedEveryone: Boolean?,
        val type: ReportType,
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
        val options: ServiceReportOptions
    )
}
