package com.metriql.dbt

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.hubspot.jinjava.interpret.TemplateError
import com.metriql.dbt.DbtManifest.Companion.getModelName
import com.metriql.report.data.recipe.Recipe
import com.metriql.util.JsonHelper
import com.metriql.util.JsonPath
import com.metriql.util.JsonUtil
import com.metriql.util.MetriqlException
import com.metriql.util.YamlHelper
import com.metriql.warehouse.spi.DataSource
import io.netty.handler.codec.http.HttpResponseStatus
import org.rakam.server.http.HttpServer
import java.io.File

object DbtYamlParser {
    private val templated_yml_suffixes = arrayOf(".jinja", ".jinja2")

    val jsonMapper: ObjectMapper = JsonHelper.getMapper().copy().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    fun parseProjectConfig(text: String): ProjectYaml {
        return try {
            YamlHelper.mapper.readValue(text, ProjectYaml::class.java)
        } catch (e: Exception) {
            throw ParseException(JsonUtil.convertToUserFriendlyError(e))
        }
    }

    fun parseMetriqlConfig(text: String): Recipe.Config {
        return try {
            YamlHelper.mapper.readValue(text, Recipe.Config::class.java)
        } catch (e: Exception) {
            throw ParseException(JsonUtil.convertToUserFriendlyError(e))
        }
    }

    fun compileJinjaFiles(
        dataSource: DataSource,
        dbtModels: List<Recipe.RecipeModel>,
        packageName: String,
        mainPath: File,
        files: Map<String, String>,
        vars: Map<String, Any?>,
        sourcePaths: List<String>
    ): MutableList<Recipe.RecipeModel> {
        val dataFiles = files.filter { file ->
            sourcePaths.any { file.key.startsWith("$it/") }
        }

        val renderer = DbtJinjaRenderer()

        val models = mutableListOf<Recipe.RecipeModel>()
        val errors = mutableListOf<MetriqlException>()

        dataFiles.filter { file -> templated_yml_suffixes.any { file.key.endsWith(it) } }.forEach { (path, content) ->
            val compiledYml = renderer.render(dataSource, content, mainPath, packageName, path, vars, files)
            val fatalErrors = compiledYml.errors.filter { it.severity == TemplateError.ErrorType.FATAL }
            fatalErrors
                .forEach { ex ->
                    val message = ex.exception?.cause?.let { it.message + " : " + (it.cause?.message ?: "Unknown Error") }
                    errors.add(MetriqlException("Error compiling file $path: \n $message", HttpResponseStatus.BAD_REQUEST))
                }

            if (fatalErrors.isEmpty()) {
                try {
                    compiledYml.output.split(DbtJinjaRenderer.delimiter).mapIndexedNotNull { idx: Int, content: String ->
                        if (idx > 0) {
                            val configAndSql = content.split("\n".toRegex(), 2)
                            val configTree = jsonMapper.readTree(configAndSql[0]) as ObjectNode
                            val sql = configAndSql[1].trim()
                            if (sql.isNotEmpty()) {
                                configTree.put("sql", sql)
                            }

                            val extends = configTree["extends"]?.asText()
                            val parent = if (extends != null) {
                                dbtModels.find { it.name == extends } ?: throw MetriqlException("Unable to find model $extends", HttpResponseStatus.BAD_REQUEST)
                            } else {
                                Recipe.RecipeModel(null)
                            }

                            val model = jsonMapper.convertValue(configTree, Recipe.RecipeModel::class.java)
                            model.copy(
                                _path = path,
                                measures = (parent.measures ?: mapOf()) + (model.measures ?: mapOf()),
                                dimensions = (parent.dimensions ?: mapOf()) + (model.dimensions ?: mapOf()),
                                relations = (parent.relations ?: mapOf()) + (model.relations ?: mapOf()),
                                mappings = model.mappings ?: parent.mappings,
                                target = model.target ?: parent.target
                            )
                        } else null
                    }.forEach {
                        models.add(it.copy(name = getModelName("view", packageName, it.name!!)))
                    }
                } catch (e: Exception) {
                    val (jsonPath, message) = JsonUtil.convertToUserFriendlyError(e)
                    errors.add(
                        MetriqlException(
                            listOf(HttpServer.JsonAPIError.codeTitle("interpret", "Unable to parse file $path in `$jsonPath` : $message}")),
                            jsonPath?.let { mapOf("path" to it) } ?: mapOf(),
                            HttpResponseStatus.BAD_REQUEST
                        )
                    )
                }
            }
        }

        if (errors.isNotEmpty()) {
            throw MetriqlException(errors.flatMap { it.errors }, mapOf(), HttpResponseStatus.BAD_REQUEST)
        }

        return models
    }

    fun encode(yml: DbtSchemaYaml): String = YamlHelper.mapper.writeValueAsString(yml)

    class ParseException(val errors: Pair<JsonPath?, String>) : Exception(errors.toString())
}
