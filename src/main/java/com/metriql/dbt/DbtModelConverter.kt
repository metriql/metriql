package com.metriql.dbt

import com.metriql.db.FieldType
import com.metriql.dbt.DbtSchemaYaml.DbtModel.DbtModelColumn.ColumnMeta
import com.metriql.report.Recipe
import com.metriql.service.model.Model
import com.metriql.util.MetriqlException
import com.metriql.util.TextUtil.toSlug
import com.metriql.util.merge
import com.metriql.util.toSnakeCase
import io.netty.handler.codec.http.HttpResponseStatus

object DbtModelConverter {
    fun toModel(model: Recipe.RecipeModel, columnList: List<DbtSchemaYaml.DbtModel.DbtModelColumn>, meta: ModelMeta?): Recipe.RecipeModel? {
        val metaModel = meta?.metriql?.copy(name = model.name)

        val relations = columnList.flatMap { col ->
            col.tests?.mapNotNull {
                if (it is DbtSchemaYaml.DbtModel.DbtModelColumn.DbtModelColumnTest.Relationships)
                    parseRef(it.to) to it.toRelation(col.name)
                else null
            } ?: listOf()
        }.toMap()

        val columnMeasures = columnList.mapNotNull { column ->
            column.meta?.measure?.let { measure ->
                val defaultMeasureName = (measure.aggregation?.let { it.toSnakeCase + "_" } ?: "") + toSlug(column.name, true)
                (measure.name ?: defaultMeasureName) to measure.copy(label = measure.label ?: column.name)
            }
        }.toMap()
        val columnDimensions = columnList.mapNotNull { col -> getDimension(col, metaModel)?.let { it.name!! to it } }.toMap()

        val definedDimensions = (metaModel?.dimensions ?: mapOf()).mapValues { dimension ->
            if (dimension.value.column != null) {
                val column = columnDimensions.entries.find { it.value.column == dimension.value.column }
                if (column != null) {
                    dimension.value.merge(column.value)
                } else {
                    dimension.value
                }
            } else {
                dimension.value
            }
        }

        val dimensions = columnDimensions + definedDimensions

        val finalModel = metaModel?.let { model.merge(it) } ?: model
        return finalModel.copy(
            measures = (finalModel.measures ?: mapOf()) + columnMeasures,
            relations = (metaModel?.relations ?: mapOf()) + relations,
            dimensions = dimensions
        )
    }

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

    private fun getDimension(column: DbtSchemaYaml.DbtModel.DbtModelColumn, meta: Recipe.RecipeModel?): Recipe.RecipeModel.Metric.RecipeDimension? {
        val defaultType = if (meta?.mappings?.get(Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP) == column.name) FieldType.TIMESTAMP else null
        val dimension = column.meta?.dimension ?: return null

        val type = if (dimension.type == null && defaultType != null) defaultType else dimension.type
        return dimension.copy(column = column.name, type = type, name = dimension.name ?: toSlug(column.name, true))
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
