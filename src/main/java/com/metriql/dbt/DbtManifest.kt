package com.metriql.dbt

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonEnumDefaultValue
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.metriql.dbt.DbtManifest.Node.TestMetadata.DbtModelColumnTest.AcceptedValues
import com.metriql.dbt.DbtManifest.Node.TestMetadata.DbtModelColumnTest.AnyValue
import com.metriql.dbt.DbtManifest.Node.TestMetadata.DbtModelColumnTest.Relationships
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.data.recipe.Recipe.RecipeModel.Companion.fromDimension
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.DiscoverService.Companion.createDimensionsFromColumns
import com.metriql.service.model.Model
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.TextUtil
import com.metriql.util.TextUtil.toUserFriendly
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.DataSource
import io.netty.handler.codec.http.HttpResponseStatus
import kotlin.reflect.KClass

typealias MetricFields = Pair<Map<String, Recipe.RecipeModel.Metric.RecipeMeasure>, Map<String, Recipe.RecipeModel.Metric.RecipeDimension>>

data class DbtManifest(val nodes: Map<String, Node>, val sources: Map<String, Source>, val generated_at: String?, val child_map: Map<String, List<String>>) {
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
        val tags: List<String>,
        val description: String,
        val meta: Meta,
        val docs: Docs,
        val columns: Map<String, DbtColumn>,
        // only available for tests
        val column_name: String?,
        val test_metadata: TestMetadata?,
    ) {
        data class Meta(@JsonAlias("rakam") val metriql: Recipe.RecipeModel?)
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
                data class Relationships(
                    val to: String,
                    val model: String,
                    val field: String,
                    val column_name: String,
                    val name: String?,
                    @JsonAlias("rakam")
                    val metriql: Recipe.RecipeModel.RecipeRelation?,
                ) : DbtModelColumnTest() {

                    @JsonIgnore
                    fun getSourceModelName(packageName: String): String? {
                        return DbtJinjaRenderer.renderer.renderReference(model, packageName)
                    }

                    @JsonIgnore
                    fun getTargetModelName(packageName: String): String {
                        return DbtJinjaRenderer.renderer.renderReference("{{$to}}", packageName)
                    }

                    @JsonIgnore
                    fun getReferenceLabel(packageName: String): String {
                        return name ?: DbtJinjaRenderer.renderer.getReferenceLabel("{{$to}}", packageName)
                    }

                    fun toRelation(packageName: String): Recipe.RecipeModel.RecipeRelation? {
                        if (metriql == null) return null
                        return Recipe.RecipeModel.RecipeRelation(
                            sourceColumn = column_name,
                            targetColumn = field,
                            to = to,
                            type = metriql.type,
                            relationship = metriql.relationship,
                            hidden = metriql.hidden,
                            description = metriql.description,
                            label = metriql.label ?: getReferenceLabel(packageName)
                        )
                    }
                }
            }

            @UppercaseEnum
            enum class Test(private val configClass: KClass<out DbtModelColumnTest>) : StrValueEnum {
                UNIQUE(AnyValue::class),
                NOT_NULL(AnyValue::class),
                ACCEPTED_VALUES(AcceptedValues::class),
                RELATIONSHIPS(Relationships::class),
                @JsonEnumDefaultValue
                UNKNOWN(AnyValue::class);

                override fun getValueClass() = configClass.java
            }

            fun applyTestToModel(model: Recipe.RecipeModel, packageName: String): Recipe.RecipeModel {
                return when (kwargs) {
                    is Relationships -> {
                        if (kwargs.getSourceModelName(packageName) == model.name) {
                            val toRelation = kwargs.toRelation(packageName)
                            if (toRelation != null) {
                                val reference = toRelation.name ?: kwargs.getReferenceLabel(packageName)
                                model.copy(relations = (model.relations ?: mapOf()) + mapOf(reference to toRelation))
                            } else {
                                model
                            }
                        } else model
                    }
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

        data class Config(val enabled: Boolean, val materialized: String, val meta : Meta?) {
            companion object {
                const val EPHEMERAL = "ephemeral"
            }
        }

        companion object {
            const val MODEL_RESOURCE_TYPE = "model"
            const val SEED_RESOURCE_TYPE = "seed"
        }

        fun toModel(datasource: DataSource, dbtManifest: DbtManifest): Recipe.RecipeModel? {
            val metriql = meta().metriql
            if (
                metriql == null ||
                (resource_type != MODEL_RESOURCE_TYPE && resource_type != SEED_RESOURCE_TYPE) ||
                !config.enabled ||
                tags.contains(DbtModelService.tagName)
            ) return null

            val modelName = metriql.name ?: TextUtil.toSlug("model_${package_name}_$name", true)

            val target = Model.Target.TargetValue.Table(database, schema, alias ?: name)

            val (columnMeasures, columnDimensions) = if (columns.isEmpty()) {
                val table = datasource.getTable(target.database, target.schema, target.table)
                val recipeDimensions = createDimensionsFromColumns(table.columns).map { it.name to fromDimension(it) }.toMap()
                mapOf<String, Recipe.RecipeModel.Metric.RecipeMeasure>() to recipeDimensions
            } else {
                extractFields(modelName, columns)
            }

            val dependencies = dbtManifest.child_map[unique_id] ?: listOf()

            val model = metriql.copy(
                hidden = meta.metriql?.hidden ?: if (resource_type == SEED_RESOURCE_TYPE) (docs?.show?.let { !it } ?: true) else false,
                name = modelName,
                sql = if (config.materialized == Config.EPHEMERAL) raw_sql else null,
                description = metriql.description ?: description,
                label = metriql.label ?: toUserFriendly(name),
                target = target,
                dimensions = (metriql.dimensions ?: mapOf()) + columnDimensions,
                measures = (metriql.measures ?: mapOf()) + columnMeasures,
                _path = original_file_path,
                package_name = package_name
            )

            val modelWithTests = dependencies.mapNotNull { dbtManifest.nodes[it] }
                .foldRight(model) { node, model -> node.test_metadata?.applyTestToModel(model, package_name) ?: model }

            return getModelIfApplicable(modelWithTests)
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
        val resource_type: String,
        val columns: Map<String, DbtColumn>,
        val meta: Node.Meta,
    ) {
        fun toModel(dbtManifest: DbtManifest): Recipe.RecipeModel? {
            if (resource_type != "source" || meta.metriql == null) return null

            val modelName = meta.metriql?.name ?: TextUtil.toSlug("source_${package_name}_${source_name}_$name", true)

            val (columnMeasures, columnDimensions) = extractFields(modelName, columns)

            val dependencies = dbtManifest.child_map[unique_id] ?: listOf()
            val model = meta.metriql.copy(
                name = modelName,
                label = meta.metriql.label ?: toUserFriendly(name),
                target = Model.Target.TargetValue.Table(database, schema, identifier),
                description = meta.metriql.description ?: description,
                dimensions = (meta.metriql.dimensions ?: mapOf()) + columnDimensions,
                measures = (meta.metriql.measures ?: mapOf()) + columnMeasures,
                _path = original_file_path,
                package_name = package_name
            )

            val finalModel = dependencies.mapNotNull { dbtManifest.nodes[it] }
                .foldRight(model) { node, model -> node.test_metadata?.applyTestToModel(model, package_name) ?: model }

            return getModelIfApplicable(finalModel)
        }
    }

    data class DbtColumn(val name: String, val description: String?, val data_type: String?, val tags: List<String>, val quote: Boolean?, val meta: DbtColumnMeta) {
        data class DbtColumnMeta(
            @JsonProperty("metriql.dimension") @JsonAlias("rakam.dimension") val dimension: Recipe.RecipeModel.Metric.RecipeDimension?,
            @JsonProperty("metriql.measure") @JsonAlias("rakam.measure") val measure: Recipe.RecipeModel.Metric.RecipeMeasure?
        )
    }

    companion object {
        fun getModelIfApplicable(model: Recipe.RecipeModel): Recipe.RecipeModel? {
            if (model.measures?.isEmpty() == true && model.dimensions?.isEmpty() == true) {
                return null
            }

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
                    val measureName = it.value.meta?.measure?.name ?: throw MetriqlException("Measure name is not set for column `$modelName`.`${it.value.name}`", HttpResponseStatus.BAD_REQUEST)
                    measureName to measure
                }
            }.toMap()

            val columnDimensions = columns.map {
                val dimensionName = it.value.meta?.dimension?.name ?: TextUtil.toSlug(it.key, true)
                val dim = (it.value.meta.dimension ?: Recipe.RecipeModel.Metric.RecipeDimension()).copy(column = it.key, description = it.value.description)
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
