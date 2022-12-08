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
import com.metriql.report.data.FilterValue
import com.metriql.report.data.ReportMetric
import com.metriql.report.segmentation.SegmentationMaterialize
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.DatasetName
import com.metriql.service.dataset.DimensionName
import com.metriql.service.dataset.RelationName
import com.metriql.service.jinja.SQLRenderable
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.UppercaseEnum
import com.metriql.util.serializableName
import com.metriql.util.toSnakeCase
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceQuery
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import kotlin.reflect.KClass

data class Recipe(
    val repository: String,
    val branch: String,
    val path: String? = null,
    val config: Config,
    val packageName: String?,
    val dependencies: Dependencies? = null,
    val models: List<RecipeModel>? = null,
    val reports: List<SaveReport>? = null,
) {
    @JsonIgnoreProperties
    fun getDependenciesWithFallback(): Dependencies {
        return dependencies ?: Dependencies()
    }

    @JSONBSerializable
    data class Dependencies(val dbt: DbtDependency? = null) {
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
                                FilterValue.MetricFilter(
                                    ReportMetric.ReportDimension(
                                        filter.dimension,
                                        filter.datasetName ?: modelName,
                                        filter.relationName,
                                        filter.timeframe
                                    ).toMetricReference(),
                                    filter.operator!!.name, filter.value
                                )
                            }

                            filter.mappingDimension != null -> {
                                FilterValue.MetricFilter(
                                    ReportMetric.ReportMappingDimension(
                                        filter.mappingDimension,
                                        filter.timeframe
                                    ).toMetricReference(),
                                    filter.operator!!.name, filter.value

                                )
                            }

                            filter.sql != null -> {
                                FilterValue.SqlFilter(filter.sql)
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
                    @JsonTypeIdResolver(FilterValue.MetricFilter.OperatorTypeResolver::class)
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
    ) {
        @JsonIgnore
        fun toFilter(
            context: IQueryGeneratorContext,
            datasetName: DatasetName,
        ): FilterValue.MetricFilter {
            var metricValue = when {
                dimension != null ->
                    dimension.toDimension(datasetName, dimension.getType(context, datasetName).second)

                measure != null ->
                    measure.toMeasure(datasetName)

                mapping != null -> {
                    val type = JsonHelper.convert(mapping, Dataset.MappingDimensions.CommonMappings::class.java)
                    ReportMetric.ReportMappingDimension(type, null)
                }

                else -> {
                    throw IllegalStateException("One of dimension, measure or mapping is required")
                }
            }
            return FilterValue.MetricFilter(metricValue.toMetricReference(), operator, value)
        }
    }

    data class FieldReference(val name: String, val relation: String? = null, val timeframe: String? = null) {
        @JsonValue
        override fun toString(): String {
            val ref = if (relation != null) "$relation.$name" else name
            return if (timeframe != null) "$ref::$timeframe" else ref
        }

        fun getType(context: IQueryGeneratorContext, datasetName: DatasetName): Pair<KClass<out ReportMetric>, FieldType> {
            val currentModel = context.getModel(datasetName)

            val targetModel = if (relation != null) {
                val targetModelName = currentModel.relations.find { r -> r.name == relation }?.datasetName
                    ?: throw MetriqlException("Relation `$relation` could not found", BAD_REQUEST)
                context.getModel(targetModelName)
            } else currentModel

            val dimensionName = getMappingDimensionIfApplicable()?.let {
                targetModel.mappings.get(name.substring(1))
            } ?: name

            val measureModel = if (relation != null) {
                context.getModel(currentModel.relations.find { it.name == relation }?.datasetName!!)
            } else currentModel

            val measure = measureModel.measures.find { it.name == name }
            val dimension = targetModel.dimensions.find { dimension -> dimension.name == dimensionName }

            val type = if (measure != null) {
                ReportMetric.ReportMeasure::class
            } else if (dimension != null) {
                ReportMetric.ReportDimension::class
            } else {
                throw MetriqlException("Metric `$this` could not found", BAD_REQUEST)
            }

            return type to (measure?.fieldType ?: dimension?.fieldType ?: FieldType.UNKNOWN)
        }

        @JsonIgnore
        fun getMappingDimensionIfApplicable(): String? {
            return if (name.startsWith(":")) {
                return name.substring(1)
            } else null
        }

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
