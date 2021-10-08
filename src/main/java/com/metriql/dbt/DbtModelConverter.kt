package com.metriql.dbt

import com.metriql.dbt.DbtSchemaYaml.DbtModel.DbtModelColumn.ColumnMeta
import com.metriql.report.data.recipe.Recipe
import com.metriql.util.MetriqlException
import com.metriql.util.TextUtil.toSlug
import io.netty.handler.codec.http.HttpResponseStatus

object DbtModelConverter {

    fun parseRef(rawRef: String): String {
        val ref = rawRef.replace(" ", "").toLowerCase()
        return when {
            ref.startsWith("ref(") -> {
                ref.substring(5, ref.length - 2)
            }
            ref.startsWith("source(") -> {
                val sources = ref.substring(8, ref.length - 2).split("','", ignoreCase = true, limit = 2)
                DbtSchemaYaml.DbtSource.getModelNameFromSource(sources[0], sources[1])
            }
            else -> {
                throw MetriqlException("Unable to resolve ref $rawRef", HttpResponseStatus.BAD_REQUEST)
            }
        }
    }

    fun fromModels(models: List<Recipe.RecipeModel>): DbtSchemaYaml {
        val sources = models.filter { it.target != null }.groupBy { Pair(it.target?.database, it.target?.schema) }.map { databaseSchema ->
            DbtSchemaYaml.DbtSource(
                "${databaseSchema.key.first?.let { "${it}_" }}${databaseSchema.key.second}",
                loadedAtField = null,
                database = databaseSchema.key.first,
                schema = databaseSchema.key.second,
                quoting = null,
                tests = null,
                meta = null,
                tables = databaseSchema.value.map { model ->
                    val meta = ModelMeta(
                        model.copy(
                            dimensions = null, description = null, name = null, target = null,
                            measures = model.measures?.filter { it.value.dimension == null && it.value.column == null }
                        )
                    )
                    DbtSchemaYaml.DbtSource.DbtTable(
                        name = model.name!!,
                        identifier = if (model.name != model.target?.table) model.target?.table else null,
                        description = model.description,
                        meta = meta,
                        columns = model.dimensions?.filter { it.value.column != null }?.map {
                            val column = it.value.column!!
                            val dimensionName = toSlug(column, true)
                            val measureValues = model.measures?.entries ?: listOf()
                            val measure = measureValues.find { m -> m.value.dimension == dimensionName } ?: measureValues.find { m -> m.value.column == column }

                            DbtSchemaYaml.DbtModel.DbtModelColumn(
                                name = column,
                                description = it.value.description,
                                meta = ColumnMeta(
                                    it.value.copy(
                                        name = if (dimensionName != column) dimensionName else null,
                                        description = null, column = null, sql = null, primary = null
                                    ),
                                    measure?.value?.copy(dimension = null, column = null, type = null, name = measure?.key)
                                ),
                                tests = null
                            )
                        }
                    )
                },
            )
        }

        return DbtSchemaYaml(version = "2", models = null, sources, seeds = null)
    }
}
