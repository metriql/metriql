package com.metriql.dbt

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.metriql.report.Recipe
import com.metriql.report.Recipe.RecipeModel.Metric.RecipeDimension
import com.metriql.report.Recipe.RecipeModel.Metric.RecipeMeasure
import com.metriql.service.model.Model.Relation.JoinType
import com.metriql.service.model.Model.Relation.RelationType
import com.metriql.util.JsonHelper
import com.metriql.util.TextUtil
import com.metriql.util.UppercaseEnum
import java.util.HashMap

typealias ColumnDimension = RecipeDimension
typealias ColumnMeasure = RecipeMeasure

// schema.yml
data class DbtSchemaYaml(val version: String? = "2", val models: List<DbtModel>?, val sources: List<DbtSource>?, val seeds: List<DbtSeed>?) {
    data class DbtSource(
        val name: String,
        val loadedAtField: String?,
        val database: String?,
        val schema: String?,
        val quoting: Quoting?,
        val meta: ModelMeta?,
        val tests: List<Any?>?,
        val tables: List<DbtTable>?,
    ) {
        data class DbtTable(val name: String, val identifier: String?, val description: String?, val meta: ModelMeta?, val columns: List<DbtModel.DbtModelColumn>?)
        data class Quoting(val database: Boolean?, val schema: Boolean?, val identifier: Boolean?)

        companion object {
            fun getModelNameFromSource(sourceName: String, tableName: String): String {
                return "${TextUtil.toSlug(sourceName)}__${TextUtil.toSlug(tableName)}"
            }
        }
    }

    data class DbtSeed(
        val name: String,
        val description: String?,
        val meta: ModelMeta?,
        val docs: Docs?,
        val columns: List<DbtModel.DbtModelColumn>?,
        val tests: List<Any?>?
    ) {
        data class Docs(val show: Boolean?)
    }

    data class DbtModel(val name: String, val description: String?, val meta: ModelMeta?, val columns: List<DbtModelColumn>?, val tests: List<Any?>?) {
        data class DbtModelColumn(
            val name: String, // column name
            val description: String? = null,
            val quote: Boolean? = null,
            val tags: List<String>? = null,
            val meta: ColumnMeta? = null,
            val tests: List<DbtModelColumnTest>? = null,
        ) {
            @JsonInclude(JsonInclude.Include.NON_NULL)
            data class ColumnMeta(
                @JsonProperty("metriql.dimension") val dimension: ColumnDimension?,
                @JsonProperty("metriql.measure") val measure: ColumnMeasure?
            )

            sealed class DbtModelColumnTest {
                class StringValue(val value: Test) : DbtModelColumnTest() {
                    @UppercaseEnum
                    enum class Test {
                        UNIQUE, NOT_NULL
                    }
                }

                data class AcceptedValues(val values: List<String>) : DbtModelColumnTest()
                data class Relationships(val to: String, val field: String, val label: String?, val description: String?, val relation: RelationType?, val type: JoinType?) :
                    DbtModelColumnTest() {
                    @JsonIgnore
                    fun toRelation(colName: String) = Recipe.RecipeModel.RecipeRelation(
                        source = colName, model = DbtModelConverter.parseRef(to),
                        target = field, type = type, relationship = relation
                    )
                }

                object Unknown : DbtModelColumnTest()

                companion object {
                    @JvmStatic
                    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
                    fun parseStr(str: String): DbtModelColumnTest {
                        return try {
                            StringValue(JsonHelper.convert(str, StringValue.Test::class.java))
                        } catch (e: Exception) {
                            Unknown
                        }
                    }

                    @JvmStatic
                    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
                    fun parseObj(relationships: Relationships?, acceptedValues: AcceptedValues?): DbtModelColumnTest {
                        return relationships ?: acceptedValues ?: Unknown
                    }
                }
            }
        }
    }
}

// dbt_project.yml
data class ProjectYaml(
    val name: String,
    val configVersion: Integer?,
    val sourcePaths: List<String>?,
    val macroPaths: List<String>?,
    val profile: String?,
    val analysisPaths: List<String>?,
    val quoting: Quote?,
    val vars: Map<String, Any?>?,
    val dataPaths: List<String>?,
    val seeds: SeedConfigs?,
    val models: ModelConfigs?,
) {
    data class Quote(val database: Boolean?, val schema: Boolean?, val identifier: Boolean?)

    @JsonIgnore
    fun getMacroPath() = macroPaths ?: listOf("macros")

    @JsonIgnore
    fun getDataPath() = dataPaths ?: listOf("data")

    @JsonIgnore
    fun getSourcePath() = sourcePaths ?: listOf("models")

    @JsonIgnore
    fun getProfiles() = profile ?: "default"
}

data class ModelMeta(@JsonAlias("rakam") val metriql: Recipe.RecipeModel?)

class SeedConfigs : GenericConfig() {
    fun find(project: String, path: String, prefixes: List<String>) = find(project, path, prefixes, SeedConfig::class.java)
}

class ModelConfigs : GenericConfig() {
    fun find(project: String, path: String, prefixes: List<String>) = find(project, path, prefixes, ModelConfig::class.java)
}

data class SeedConfig(
    val column_types: String? = null,
    val schema: String? = null,
    val quote_columns: String? = null,
    val database: String? = null,
    val tags: String? = null,
)

data class ModelConfig(
    val enabled: Boolean? = null,
    val schema: String? = null,
    val database: String? = null,
    val tags: String? = null,
    val materialized: String? = null,
) {
    companion object {
        fun isEphemeral(config: ModelConfig) = config.materialized?.toLowerCase().equals("ephemeral")
    }
}

open class GenericConfig : HashMap<String, Any>() {
    protected fun <T> find(projectName: String, path: String, prefixes: List<String>, type: Class<T>): T {
        val props = mutableMapOf<String, Any?>()

        val prefix = prefixes.find { prefixes.any { path.startsWith(it) } }

        if (prefix != null) {
            val paths = path.substring(prefix.length + 1).split("/")

            var current = this as Map<String, *>?
            for (element in paths) {
                if (current == null) {
                    break
                }

                current.filter { (it.key as? String)?.startsWith("+") ?: false }.forEach {
                    props[it.key.substring(1)] = it.value
                }

                current = current[element] as? Map<String, *>?
            }
        }

        return DbtYamlParser.jsonMapper.convertValue(props, type)
    }
}
