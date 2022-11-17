package com.metriql.dbt

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonEnumDefaultValue
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter
import com.fasterxml.jackson.annotation.Nulls
import com.metriql.dbt.DbtManifest.Node.TestMetadata.DbtModelColumnTest.AcceptedValues
import com.metriql.dbt.DbtManifest.Node.TestMetadata.DbtModelColumnTest.AnyValue
import com.metriql.report.data.recipe.Recipe.RecipeModel
import com.metriql.report.data.recipe.Recipe.RecipeModel.Companion.fromDimension
import com.metriql.report.data.recipe.Recipe.RecipeModel.Metric.RecipeMeasure.Filter
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.dataset.DiscoverService.Companion.createDimensionsFromColumns
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.TIME_SERIES
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.TextUtil
import com.metriql.util.TextUtil.toUserFriendly
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.DataSource
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND
import kotlin.reflect.KClass

typealias MetricFields = Pair<Map<String, RecipeModel.Metric.RecipeMeasure>, Map<String, RecipeModel.Metric.RecipeDimension>>

data class DbtManifest(
    val nodes: Map<String, Node>,
    val sources: Map<String, Source>,
    val generated_at: String?,
    val child_map: Map<String, List<String>>?,
    val metrics: Map<String, Metric>?
) {
    data class Metric(
        val unique_id: String,
        val package_name: String,
        val path: String,
        val model: String,
        val name: String,
        val description: String?,
        val label: String?,
        val type: String,
        val sql: String?,
        val timestamp: String?,
        val time_grains: List<String>,
        val dimensions: List<String>?,
        val filters: List<Filter>?,
        val meta: Node.Meta,
        val tags: List<String>?,
        val original_file_path: String,
    ) {
        fun toModel(manifest: DbtManifest?, modelsAndSources: List<RecipeModel>): RecipeModel? {
            val datasetName = "metric_${package_name}_$name"
            val sourceModelName = DbtJinjaRenderer.renderer.renderReference("{{$model}}", package_name)
            val sourceModel = modelsAndSources.find { it.name == sourceModelName }
                ?: throw MetriqlException("Unable to find `$name` metric's parent model `$model`", NOT_FOUND)

            val measureFilters = filters?.let { it.map { filter -> Filter(filter.field, operator = filter.convertPostOperation(sourceModel, name), value = filter.value) } }

            val mappings = sourceModel.mappings ?: Dataset.MappingDimensions()
            if (timestamp != null) {
                mappings.put(TIME_SERIES, timestamp)
            }

            val existingDimensions = sourceModel.dimensions?.map {
                it.key to if (dimensions?.contains(it.key) == false) {
                    it.value.copy(hidden = true)
                } else it.value
            }?.toMap() ?: mapOf()

            val modelDimensions = if (time_grains != null) {
                val eventTimestamp = mappings.get(TIME_SERIES)
                val dimension = sourceModel.dimensions?.get(eventTimestamp)?.copy(timeframes = time_grains)
                    ?: throw MetriqlException("Unable to find timestamp column $timestamp for metric $name", NOT_FOUND)
                existingDimensions + mapOf(eventTimestamp!! to dimension)
            } else existingDimensions

            val relations = dimensions?.let { dimensions ->
                sourceModel.relations?.map { rel ->
                    val modelName = rel.value.getModel(package_name, rel.key)
                    val targetModel = modelsAndSources.find { it.name == modelName }
                        ?: throw MetriqlException("Unable to find `${rel.key}` metric's relation model `$modelName`", NOT_FOUND)
                    val availableFields = targetModel.dimensions?.filter { dimensions.contains("${rel.key}.${it.value}") }?.map { it.key }?.toSet()
                    rel.key to rel.value.copy(fields = availableFields)
                }?.toMap()
            } ?: sourceModel.relations

            val metriqlType = try {
                JsonHelper.convert(type, DbtMetric::class.java).type
            } catch (e: Exception) {
                JsonHelper.convert(type, Dataset.Measure.AggregationType::class.java)
            }

            val label = label ?: meta.metriql?.label
            val model = sourceModel.copy(
                name = datasetName,
                relations = relations,
                hidden = meta.metriql?.hidden ?: sourceModel.hidden,
                description = description ?: meta.metriql?.description,
                label = label ?: name,
                dimensions = modelDimensions,
                measures = mapOf(name to RecipeModel.Metric.RecipeMeasure(label = label, aggregation = metriqlType, sql = sql, filters = measureFilters)),
                tags = tags ?: sourceModel.tags,
                _path = original_file_path,
                package_name = package_name
            )

            return fixJinjaExpressions(model)
        }

        data class Filter(val field: String, val operator: String, val value: Any?) {
            fun convertPostOperation(model: RecipeModel, metricName: String): Enum<*> {
                val dim = model.dimensions?.get(field) ?: throw MetriqlException("Unable to find filter $field in $metricName", NOT_FOUND)
                val typeClass = dim.type?.operatorClass?.java ?: throw MetriqlException("type is required for dimension $field in metric $metricName", NOT_FOUND)
                return JsonHelper.convert(operator, typeClass)
            }
        }
    }

    @UppercaseEnum
    enum class DbtMetric(val type: Dataset.Measure.AggregationType) {
        COUNT_DISTINCT(Dataset.Measure.AggregationType.COUNT_UNIQUE),
        MAX(Dataset.Measure.AggregationType.MAXIMUM),
        MIN(Dataset.Measure.AggregationType.MINIMUM),
        AVG(Dataset.Measure.AggregationType.AVERAGE)
    }

    data class Node(
        val database: String?,
        val schema: String?,
        val raw_sql: String,
        val unique_id: String,
        val config: Config,
        val name: String,
        val path: String,
        val alias: String?,
        val original_file_path: String,
        val package_name: String,
        val resource_type: String,
        val tags: List<String>?,
        val description: String,
        val meta: Meta,
        val docs: Docs,
        val columns: Map<String, DbtColumn>,
        // only available for tests
        val column_name: String?,
        val test_metadata: TestMetadata?,
    ) {
        data class Meta(@JsonAlias("rakam") @JsonSetter(nulls = Nulls.AS_EMPTY) val metriql: RecipeModel?)
        data class Docs(val show: Boolean?)

        fun meta(): Meta {
            return config.meta ?: meta
        }

        data class TestMetadata(
            val namespace: String?,
            val name: Test?,
            @PolymorphicTypeStr<Test>(externalProperty = "name", valuesEnum = Test::class)
            val kwargs: DbtModelColumnTest?
        ) {

            sealed class DbtModelColumnTest {
                object AnyValue : DbtModelColumnTest()

                data class AcceptedValues(val values: List<String>) : DbtModelColumnTest()
            }

            @UppercaseEnum
            enum class Test(private val configClass: KClass<out DbtModelColumnTest>) : StrValueEnum {
                UNIQUE(AnyValue::class),
                NOT_NULL(AnyValue::class),
                ACCEPTED_VALUES(AcceptedValues::class),

                @JsonEnumDefaultValue
                UNKNOWN(AnyValue::class);

                override fun getValueClass() = configClass.java
            }

            fun applyTestToModel(model: RecipeModel): RecipeModel {
                return when (kwargs) {
                    is AcceptedValues -> model
                    is AnyValue -> {
                        when (this.name) {
                            Test.UNIQUE -> model
                            else -> model
                        }
                    }
                    else -> model
                }
            }
        }

        data class Config(val enabled: Boolean, val materialized: String, val meta: Meta?) {
            companion object {
                const val EPHEMERAL = "ephemeral"
            }
        }

        companion object {
            const val MODEL_RESOURCE_TYPE = "model"
            const val SEED_RESOURCE_TYPE = "seed"
        }

        fun toModel(datasource: DataSource, dbtManifest: DbtManifest): RecipeModel? {
            val metriql = meta().metriql
            if (
                metriql == null ||
                (resource_type != MODEL_RESOURCE_TYPE && resource_type != SEED_RESOURCE_TYPE) ||
                !config.enabled ||
                tags?.contains(DbtModelService.tagName) == true
            ) return null

            val modelName = TextUtil.toSlug("model_${package_name}_$name", true)
            val target = Dataset.Target.TargetValue.Table(database, schema, alias ?: name)

            val (columnMeasures, columnDimensions) = if (columns.isEmpty()) {
                val table = datasource.getTableSchema(target.database, target.schema, target.table)
                val recipeDimensions = createDimensionsFromColumns(table.columns).associate { it.name to fromDimension(it) }
                mapOf<String, RecipeModel.Metric.RecipeMeasure>() to recipeDimensions
            } else {
                extractFields(modelName, columns)
            }

            val dependencies = dbtManifest.child_map?.get(unique_id) ?: listOf()

            val model = metriql?.copy(
                hidden = meta.metriql?.hidden ?: if (resource_type == SEED_RESOURCE_TYPE) (docs?.show?.let { !it } ?: true) else false,
                name = modelName,
                sql = if (config.materialized == Config.EPHEMERAL) raw_sql else null,
                description = metriql.description ?: description,
                label = metriql.label ?: toUserFriendly(name),
                target = target,
                dimensions = (metriql.dimensions ?: mapOf()) + columnDimensions,
                measures = (metriql.measures ?: mapOf()) + columnMeasures,
                tags = tags ?: metriql.tags,
                _path = original_file_path,
                package_name = package_name
            )

            val modelWithTests = model?.let {
                dependencies.mapNotNull { dep -> dbtManifest.nodes[dep] }
                    .foldRight(model) { node, model -> node.test_metadata?.applyTestToModel(it) ?: model }
            }

            return fixJinjaExpressions(modelWithTests)
        }
    }

    data class Source(
        val database: String?,
        val schema: String?,
        val unique_id: String,
        val source_name: String,
        val package_name: String,
        val name: String,
        val identifier: String,
        val loaded_at_field: String?,
        val source_description: String?,
        val original_file_path: String,
        val description: String?,
        val column_name: String?,
        val tags: List<String>?,
        val resource_type: String,
        val columns: Map<String, DbtColumn>,
        val meta: Node.Meta,
    ) {
        fun toModel(dataSource: DataSource, dbtManifest: DbtManifest): RecipeModel? {
            if (resource_type != "source" || meta.metriql == null) return null

            val modelName = TextUtil.toSlug("source_${package_name}_${source_name}_$name", true)

            val dependencies = dbtManifest.child_map?.get(unique_id) ?: listOf()

            val (columnMeasures, columnDimensions) = if (columns.isEmpty()) {
                val table = try {
                    dataSource.getTableSchema(database, schema, identifier)
                } catch (e: MetriqlException) {
                    throw MetriqlException("Unable to fetch columns for $modelName: ${e.message}", e.statusCode)
                }
                val recipeDimensions = createDimensionsFromColumns(table.columns).associate { it.name to fromDimension(it) }
                mapOf<String, RecipeModel.Metric.RecipeMeasure>() to recipeDimensions
            } else {
                extractFields(modelName, columns)
            }

            val dataset = meta.metriql.copy(
                name = modelName,
                label = meta.metriql.label ?: toUserFriendly(name),
                target = Dataset.Target.TargetValue.Table(database, schema, identifier),
                description = meta.metriql.description ?: description,
                dimensions = (meta.metriql.dimensions ?: mapOf()) + columnDimensions,
                measures = (meta.metriql.measures ?: mapOf()) + columnMeasures,
                tags = tags ?: meta.metriql?.tags,
                _path = original_file_path,
                package_name = package_name
            )

            val finalModel = dependencies.mapNotNull { dbtManifest.nodes[it] }
                .foldRight(dataset) { node, model -> node.test_metadata?.applyTestToModel(model) ?: model }

            return fixJinjaExpressions(finalModel)
        }
    }

    data class DbtColumn(val name: String, val description: String?, val data_type: String?, val tags: List<String>, val quote: Boolean?, val meta: DbtColumnMeta) {
        data class DbtColumnMeta(
            @JsonProperty("metriql.dimension") @JsonAlias("rakam.dimension") val dimension: RecipeModel.Metric.RecipeDimension?,
            @JsonProperty("metriql.measure") @JsonAlias("rakam.measure") val measure: RecipeModel.Metric.RecipeMeasure?
        )
    }

    companion object {
        fun fixJinjaExpressions(model: RecipeModel): RecipeModel? {
            return model.copy(
                dimensions = model.dimensions?.map { it.key to it.value.copy(sql = it.value.sql?.let { sql -> convertToJinja(sql) }) }?.toMap(),
                measures = model.measures?.map { it.key to it.value.copy(sql = it.value.sql?.let { sql -> convertToJinja(sql) }) }?.toMap(),
                relations = model.relations?.map { it.key to it.value.copy(sql = it.value.sql?.let { sql -> convertToJinja(sql) }) }?.toMap(),
            )
        }

        private val expressionRegex = "\\{([^}]+)\\}".toRegex()
        private val tagRegex = "\\{\\$([^}]+)\\\$\\}".toRegex()
        private fun convertToJinja(sql: SQLRenderable): SQLRenderable {
            return sql.replace(expressionRegex, "{{$1}}").replace(tagRegex, "{%$1%}")
        }

        fun extractFields(modelName: String, columns: Map<String, DbtColumn>): MetricFields {
            val columnMeasures = columns.mapNotNull {
                val measure = it.value.meta.measure?.copy(column = it.key, description = it.value.description)

                if (measure == null) {
                    null
                } else {
                    val measureName =
                        it.value.meta?.measure?.name ?: throw MetriqlException("Measure name is not set for column `$modelName`.`${it.value.name}`", HttpResponseStatus.BAD_REQUEST)
                    measureName to measure
                }
            }.toMap()

            val columnDimensions = columns.map {
                val dimensionName = it.value.meta?.dimension?.name ?: TextUtil.toSlug(it.key, true)
                val dim = (it.value.meta.dimension ?: RecipeModel.Metric.RecipeDimension()).copy(column = it.key, description = it.value.description, tags = it.value.tags)
                dimensionName to dim
            }.toMap()

            return columnMeasures to columnDimensions
        }

        fun getModelName(type: String, name: String, projectName: String): String {
            return "${type}_${projectName}_$name"
        }

        fun getModelNameRegex(type: String, name: String, packageName: String?): String {
            return "${type}\\_${packageName ?: "[a-z0-9_]+"}\\_$name"
        }
    }
}
