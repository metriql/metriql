package com.metriql.dbt

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.metriql.dbt.DbtManifest.Node.TestMetadata.DbtModelColumnTest.AcceptedValues
import com.metriql.dbt.DbtManifest.Node.TestMetadata.DbtModelColumnTest.AnyValue
import com.metriql.dbt.DbtManifest.Node.TestMetadata.DbtModelColumnTest.Relationships
import com.metriql.report.Recipe
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.Model
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.TextUtil
import com.metriql.util.TextUtil.toUserFriendly
import com.metriql.util.UppercaseEnum
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
                        return DbtJinjaRenderer.renderer.getReferenceLabel("{{$to}}", packageName)
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
                UNIQUE(AnyValue::class), NOT_NULL(AnyValue::class), ACCEPTED_VALUES(AcceptedValues::class), RELATIONSHIPS(Relationships::class);

                override fun getValueClass() = configClass.java
            }

            fun applyTestToModel(model: Recipe.RecipeModel, packageName: String): Recipe.RecipeModel {
                return when (kwargs) {
                    is Relationships -> {
                        if (kwargs.getSourceModelName(packageName) == model.name) {
                            val toRelation = kwargs.toRelation(packageName)
                            if (toRelation != null) {
                                val reference = kwargs.getReferenceLabel(packageName)
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

        data class Config(val enabled: Boolean, val materialized: String) {
            companion object {
                const val EPHEMERAL = "ephemeral"
            }
        }

        companion object {
            const val MODEL_RESOURCE_TYPE = "model"
            const val SEED_RESOURCE_TYPE = "seed"
        }

        fun toModel(dbtManifest: DbtManifest): Recipe.RecipeModel? {
            if ((resource_type != MODEL_RESOURCE_TYPE && resource_type != SEED_RESOURCE_TYPE) || !config.enabled || tags.contains(DbtModelService.tagName)) return null

            val modelName = meta.metriql?.name ?: TextUtil.toSlug(unique_id, true)

            if (config.materialized == Config.EPHEMERAL) {
                val sql = raw_sql
            }

            val (columnMeasures, columnDimensions) = extractFields(columns)

            val dependencies = dbtManifest.child_map[unique_id] ?: listOf()

            val recipeModel = (meta.metriql ?: Recipe.RecipeModel(null))
            val model = recipeModel.copy(
                hidden = meta.metriql?.hidden ?: if (resource_type == SEED_RESOURCE_TYPE) (docs?.show?.let { !it } ?: true) else false,
                name = modelName,
                description = recipeModel.description ?: description,
                label = recipeModel.label ?: toUserFriendly(name),
                target = Model.Target.TargetValue.Table(database, schema, alias ?: name),
                dimensions = (recipeModel.dimensions ?: mapOf()) + columnDimensions,
                measures = (recipeModel.measures ?: mapOf()) + columnMeasures,
                _path = original_file_path
            )

            dependencies.mapNotNull { dbtManifest.nodes[it] }
                .foldRight(model) { node, model -> node.test_metadata?.applyTestToModel(model, package_name) ?: model }

            return getModelIfApplicable(model)
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
            if (resource_type != "source") return null

            val modelName = meta.metriql?.name ?: TextUtil.toSlug(unique_id, true)

            val (columnMeasures, columnDimensions) = extractFields(columns)

            val dependencies = dbtManifest.child_map[unique_id] ?: listOf()
            val recipeModel = (meta.metriql ?: Recipe.RecipeModel(null))
            val model = recipeModel.copy(
                name = modelName,
                label = recipeModel.label ?: toUserFriendly(name),
                target = Model.Target.TargetValue.Table(database, schema, identifier),
                description = recipeModel.description ?: description,
                dimensions = (recipeModel.dimensions ?: mapOf()) + columnDimensions,
                measures = (recipeModel.measures ?: mapOf()) + columnMeasures,
                _path = original_file_path
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

        fun extractFields(columns: Map<String, DbtColumn>): MetricFields {
            val columnMeasures = columns.mapNotNull {
                val measureName = it.value.meta?.measure?.name ?: TextUtil.toSlug(it.key, true)
                val measure = it.value.meta.measure?.copy(column = it.key, description = it.value.description)

                if (measure == null) {
                    null
                } else {
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

        fun getModelName(type: String, projectName: String, name: String): String {
            return "${type}_${projectName}_$name"
        }

        fun getModelNameRegex(type: String, name: String, packageName: String?): String {
            return "${type}\\_${packageName ?: "[a-z0-9_]+"}\\_$name"
        }
    }
}
