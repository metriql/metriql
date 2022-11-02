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
import com.metriql.report.segmentation.SegmentationMaterialize
import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.DimensionName
import com.metriql.service.model.Dataset
import com.metriql.service.model.Dataset.MappingDimensions.CommonMappings.TIME_SERIES
import com.metriql.service.model.DatasetName
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
import com.metriql.warehouse.spi.services.ServiceQuery
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
        val target: Dataset.Target.TargetValue.Table? = null,
        val sql: SQLRenderable? = null,
        val dataset: String? = null,
        val label: String? = null,
        val description: String? = null,
        val category: String? = null,
        val extends: String? = null,
        val mappings: Dataset.MappingDimensions? = null,
        val relations: Map<String, RecipeRelation>? = null,
        val dimensions: Map<String, Metric.RecipeDimension>? = null,
        val columns: List<DbtManifest.DbtColumn>? = null,
        val measures: Map<String, Metric.RecipeMeasure>? = null,
        val materializes: Map<String, Map<String, SegmentationMaterialize>>? = null,
        val tags: List<String>? = null,
        @JsonAlias("always_filters")
        val alwaysFilters: List<OrFilters>? = null,
        @JsonIgnore
        val _path: String? = null,
        val package_name: String? = null,
    ) {
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
            fun fromDimension(dimension: Dataset.Dimension): Metric.RecipeDimension {
                return when (dimension.value) {
                    is Dataset.Dimension.DimensionValue.Sql -> Metric.RecipeDimension(
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

                    is Dataset.Dimension.DimensionValue.Column -> Metric.RecipeDimension(
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

            fun fromModel(dataset: Dataset): RecipeModel {
                val recipeRelations = dataset.relations.map { relation ->
                    relation.name to when (relation.value) {
                        is Dataset.Relation.RelationValue.SqlValue -> RecipeRelation(
                            relation.label,
                            relation.description,
                            relation.relationType,
                            relation.joinType,
                            relation.datasetName,
                            relation.value.sql,
                            null,
                            null,
                            null,
                            null,
                            if (relation.hidden === true) true else null,
                        )

                        is Dataset.Relation.RelationValue.ColumnValue -> RecipeRelation(
                            relation.label,
                            relation.description,
                            relation.relationType,
                            relation.joinType,
                            relation.datasetName,
                            null,
                            null,
                            null,
                            relation.value.sourceColumn,
                            relation.value.targetColumn,
                            if (relation.hidden === true) true else null
                        )

                        is Dataset.Relation.RelationValue.DimensionValue -> RecipeRelation(
                            relation.label,
                            relation.description,
                            relation.relationType,
                            relation.joinType,
                            relation.datasetName,
                            null,
                            relation.value.sourceDimension,
                            relation.value.targetDimension,
                            null,
                            null,
                            if (relation.hidden === true) true else null
                        )
                    }
                }.toMap()

                val recipeDimensions = dataset.dimensions.map { dimension ->
                    dimension.name to fromDimension(dimension)
                }.toMap()

                val recipeMeasures = dataset.measures.map { measure ->
                    val filters = if (measure.filters != null) {
                        measure.filters
                            .map { filter ->
                                when (filter.value) {
                                    is ReportFilter.FilterValue.SqlFilter -> {
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
                                            when (val metricValue = it.metric) {
                                                is ReportMetric.ReportMeasure -> throw IllegalStateException("Measure filters can't filter measures")
                                                is ReportMetric.ReportDimension -> {
                                                    val dimensionType = dataset.dimensions.find { dim -> dim.name == metricValue.name }?.fieldType
                                                        ?: throw MetriqlException(
                                                            "Dimension ${dataset.name}.${metricValue.name} must have type since these is filter set on it.",
                                                            NOT_FOUND
                                                        )
                                                    val (type, operation) = getOperation(dimensionType, it.operator)
                                                    filter.value.filters
                                                        .map {
                                                            Metric.RecipeMeasure.Filter(
                                                                metricValue.name,
                                                                null,
                                                                null,
                                                                metricValue.dataset,
                                                                metricValue.relation,
                                                                metricValue.timeframe,
                                                                type,
                                                                operation,
                                                                it.value
                                                            )
                                                        }
                                                }

                                                is ReportMetric.ReportMappingDimension -> {
                                                    val name = dataset.mappings.get(metricValue.name)
                                                    val dimensionType = dataset.dimensions.find { dim -> dim.name == name }?.fieldType
                                                        ?: throw MetriqlException(
                                                            "Dimension ${dataset.name}.${metricValue.name} must have type since these is filter set on it.",
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
                                                                metricValue.timeframe,
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

                                    is ReportFilter.FilterValue.NestedFilter -> null!!
                                }
                            }.flatten()
                    } else {
                        null
                    }
                    measure.name to when (measure.value) {
                        is Dataset.Measure.MeasureValue.Sql -> {
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

                        is Dataset.Measure.MeasureValue.Column -> {
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

                        is Dataset.Measure.MeasureValue.Dimension -> {
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

                return RecipeModel(
                    dataset.name,
                    if (dataset.hidden === true) true else null,
                    dataset.target.value as? Dataset.Target.TargetValue.Table,
                    (dataset.target.value as? Dataset.Target.TargetValue.Sql)?.sql,
                    null,
                    dataset.label,
                    dataset.description,
                    dataset.category,
                    null,
                    dataset.mappings,
                    recipeRelations,
                    recipeDimensions,
                    null,
                    recipeMeasures,
                    dataset.materializes,
                    dataset.tags,
                    dataset.alwaysFilters,
                    dataset.location
                )
            }
        }

        fun toModel(packageNameFromModel: String, bridge: WarehouseMetriqlBridge): Dataset {
            val packageName = this.package_name ?: packageNameFromModel
            val modelName = name ?: throw MetriqlException("Model name is required", BAD_REQUEST)

            val modelRelations = relations?.map { (relationName, relation) -> relation.toRelation(packageName, relationName) } ?: listOf()

            val datasetTarget = when {
                sql != null -> {
                    Dataset.Target(Dataset.Target.Type.SQL, Dataset.Target.TargetValue.Sql(sql))
                }

                dataset != null -> {
                    val modelName = DbtJinjaRenderer.renderer.renderReference(dataset, packageName)
                    Dataset.Target(Dataset.Target.Type.SQL, Dataset.Target.TargetValue.Sql("select * from {{model.$modelName}}"))
                }

                else -> {
                    Dataset.Target(Dataset.Target.Type.TABLE, target!!)
                }
            }

            val (dbtMeasures, dbtDimensions) = extractFields(modelName, columns?.map { it.name to it }?.toMap() ?: mapOf())

            return Dataset(
                modelName,
                hidden = hidden ?: false,
                target = datasetTarget,
                label = label,
                description = description,
                category = category,
                mappings = mappings ?: Dataset.MappingDimensions(),
                relations = modelRelations,
                dimensions = ((dimensions ?: mapOf()) + dbtDimensions)?.map { (dimensionName, dimension) -> dimension.toDimension(dimensionName, bridge) },
                measures = ((measures ?: mapOf()) + dbtMeasures)?.map { (measureName, measure) -> measure.toMeasure(measureName, modelName) },
                materializes = materializes,
                alwaysFilters = alwaysFilters,
                tags = tags,
                location = _path
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

                fun toDimension(dimensionName: String, bridge: WarehouseMetriqlBridge): Dataset.Dimension {
                    val (valType, value) = if (sql != null) {
                        Pair(Dataset.Dimension.Type.SQL, Dataset.Dimension.DimensionValue.Sql(sql, window))
                    } else {
                        Pair(Dataset.Dimension.Type.COLUMN, Dataset.Dimension.DimensionValue.Column(column!!))
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

                    return Dataset.Dimension(
                        dimensionName,
                        valType,
                        value,
                        description,
                        label,
                        category,
                        primary,
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
                val aggregation: Dataset.Measure.AggregationType? = null,
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

                fun toMeasure(measureName: String, modelName: String): Dataset.Measure {
                    val (valType, value) = when {
                        sql != null -> {
                            Pair(Dataset.Measure.Type.SQL, Dataset.Measure.MeasureValue.Sql(sql, aggregation, window))
                        }

                        dimension != null -> {
                            val agg = aggregation ?: throw MetriqlException("Aggregation is required for $modelName.$measureName", BAD_REQUEST)
                            Pair(
                                Dataset.Measure.Type.DIMENSION,
                                Dataset.Measure.MeasureValue.Dimension(agg, dimension)
                            )
                        }

                        else -> {
                            val agg = aggregation ?: throw MetriqlException("Aggregation is required for $modelName.$measureName", BAD_REQUEST)
                            Pair(
                                Dataset.Measure.Type.COLUMN,
                                Dataset.Measure.MeasureValue.Column(agg, column)
                            )
                        }
                    }

                    val filters = filters?.map { filter ->
                        when {
                            filter.dimension != null -> {
                                ReportFilter(
                                    ReportFilter.Type.METRIC,
                                    ReportFilter.FilterValue.MetricFilter(
                                        ReportFilter.FilterValue.MetricFilter.Connector.AND,
                                        listOf(
                                            ReportFilter.FilterValue.MetricFilter.Filter(
                                                ReportFilter.FilterValue.MetricFilter.MetricType.DIMENSION, ReportMetric.ReportDimension(
                                                    filter.dimension,
                                                    filter.datasetName ?: modelName,
                                                    filter.relationName,
                                                    filter.timeframe
                                                ), filter.operator!!.name, filter.value
                                            )
                                        )
                                    )
                                )
                            }

                            filter.mappingDimension != null -> {
                                ReportFilter(
                                    ReportFilter.Type.METRIC,
                                    ReportFilter.FilterValue.MetricFilter(
                                        ReportFilter.FilterValue.MetricFilter.Connector.AND,
                                        listOf(
                                            ReportFilter.FilterValue.MetricFilter.Filter(
                                                ReportFilter.FilterValue.MetricFilter.MetricType.MAPPING_DIMENSION,
                                                ReportMetric.ReportMappingDimension(
                                                    filter.mappingDimension,
                                                    filter.timeframe
                                                ), filter.operator!!.name, filter.value
                                            )
                                        )
                                    )
                                )
                            }

                            filter.sql != null -> {
                                ReportFilter(
                                    ReportFilter.Type.SQL,
                                    ReportFilter.FilterValue.SqlFilter(filter.sql)
                                )
                            }

                            else -> throw IllegalStateException("dimension, mappingDimension or sql must be present in a measure filter")
                        }
                    }
                    return Dataset.Measure(
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
                    val mappingDimension: Dataset.MappingDimensions.CommonMappings? = null,
                    val sql: SQLRenderable? = null,
                    val datasetName: DatasetName? = null,
                    val relationName: RelationName? = null,
                    val timeframe: ReportMetric.Timeframe? = null,
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
            val relationship: Dataset.Relation.RelationType? = null,
            @JsonAlias("join", "joinType")
            val type: Dataset.Relation.JoinType? = null,
            @JsonAlias("modelName")
            val model: DatasetName? = null,
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
            fun getModel(packageName: String, relationName: String): DatasetName {
                val toModel = to?.let { DbtJinjaRenderer.renderer.renderReference("{{$it}}", packageName) }
                return model ?: toModel ?: relationName
            }

            fun toRelation(packageName: String, relationName: String): Dataset.Relation {
                val (relType, value) = when {
                    sql != null -> {
                        Pair(Dataset.Relation.Type.SQL, Dataset.Relation.RelationValue.SqlValue(sql))
                    }

                    source != null -> {
                        Pair(
                            Dataset.Relation.Type.DIMENSION,
                            Dataset.Relation.RelationValue.DimensionValue(source, target!!)
                        )
                    }

                    else -> {
                        Pair(
                            Dataset.Relation.Type.COLUMN,
                            Dataset.Relation.RelationValue.ColumnValue(sourceColumn!!, targetColumn!!)
                        )
                    }
                }

                return Dataset.Relation(
                    relationName,
                    label,
                    description,
                    relationship ?: Dataset.Relation.RelationType.ONE_TO_ONE,
                    type ?: Dataset.Relation.JoinType.LEFT_JOIN,
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

        fun getType(context: IQueryGeneratorContext, datasetName: DatasetName): FieldType {
            val currentModel = context.getModel(datasetName)

            val targetModel = if (relation != null) {
                val targetModelName = currentModel.relations.find { r -> r.name == relation }?.datasetName
                    ?: throw MetriqlException("Relation `$relation` could not found", BAD_REQUEST)
                context.getModel(targetModelName)
            } else currentModel

            val dimensionName = if (isMappingDimension()) {
                targetModel.mappings[name.substring(1)]
            } else {
                name
            }

            val measureModel = if (relation != null) {
                context.getModel(currentModel.relations.find { it.name == relation }?.datasetName!!)
            } else currentModel

            val measureType = measureModel.measures.find { it.name == name }?.fieldType

            val dimensionType =
                targetModel.dimensions.find { dimension -> dimension.name == dimensionName }?.fieldType

            return measureType ?: dimensionType ?: FieldType.UNKNOWN
        }

        @JsonIgnore
        fun isMappingDimension() = name.startsWith(":")

        fun toMeasure(datasetName: DatasetName): ReportMetric.ReportMeasure {
            if (timeframe != null) {
                throw MetriqlException("`$this`: timeframe can't be used for measure references", BAD_REQUEST)
            }
            return ReportMetric.ReportMeasure(datasetName, name, relation)
        }

        @JsonIgnore
        fun toDimension(datasetName: DatasetName, type: FieldType): ReportMetric.ReportDimension {
            return ReportMetric.ReportDimension(
                name, datasetName, relation,
                if (timeframe != null)
                    try {
                        ReportMetric.Timeframe.fromFieldType(type, timeframe)
                    } catch (e: IllegalArgumentException) {
                        throw MetriqlException("Error constructing dimension $this: ${e.message}", BAD_REQUEST)
                    }
                else null,
            )
        }

        companion object {
            private val regex = "(([A-Z0-9a-z_]+)?\\.)?(\\$?[:A-Z0-9a-z_]+)".toRegex()

            fun mappingDimension(name: Dataset.MappingDimensions.CommonMappings, relation: String?): FieldReference {
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
        val options: ServiceQuery
    )
}
