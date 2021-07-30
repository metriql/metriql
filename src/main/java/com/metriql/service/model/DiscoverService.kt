package com.metriql.service.model

import com.metriql.db.FieldType
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.DEVICE_ID
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.USER_ID
import com.metriql.util.MetriqlException
import com.metriql.util.serializableName
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.TableSchema
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import net.gcardone.junidecode.Junidecode.unidecode
import java.util.Locale

class DiscoverService(private val dataSource: DataSource) {

    fun discoverMeasuresFromScratch(dimensions: List<Model.Dimension>, mappings: Model.MappingDimensions): List<Model.Measure> {
        // This default applies for all
        val measures = mutableListOf<Model.Measure>()
        measures.add(
            Model.Measure(
                "count_of_rows",
                null,
                null,
                null,
                Model.Measure.Type.COLUMN,
                Model.Measure.MeasureValue.Column(Model.Measure.AggregationType.COUNT, null),
                null, null
            )
        )

        Model.MappingDimensions.CommonMappings.values()
            .mapNotNull { mapping -> mappings.get(mapping)?.let { mapping.discoveredMeasure.invoke(it) } }
            .forEach {
                measures.add(it)
            }

        // Add numeric dimensions of SUM(col)
        dimensions
            .filter { it.type == Model.Dimension.Type.COLUMN }
            .filter { it.fieldType?.isNumeric == true }
            .forEach {
                measures.add(
                    Model.Measure(
                        toMetriqlConventionalName("sum_of_${it.name}"),
                        null,
                        it.description,
                        null,
                        Model.Measure.Type.DIMENSION,
                        Model.Measure.MeasureValue.Dimension(Model.Measure.AggregationType.SUM, it.name),
                        null, null
                    )
                )
            }

        return measures
    }

    fun discoverDimensionFieldTypes(context: IQueryGeneratorContext, modelName: String, modelTarget: Model.Target, dimensions: List<Model.Dimension>): List<Model.Dimension> {
        val dimensionsWithoutFieldType = dimensions
            .filter { it.fieldType == null && it.hidden != true }

        if (dimensionsWithoutFieldType.isEmpty()) return dimensions

        val query = dataSource.warehouse.bridge.generateDimensionMetaQuery(
            modelName,
            modelTarget,
            dimensionsWithoutFieldType,
            context
        )

        val tableMeta = try {
            dataSource.getTable(query)
        } catch (e: Exception) {
            throw MetriqlException("Failed to run query for exploring the dimension types: ${e.message} \n $query", HttpResponseStatus.BAD_REQUEST)
        }

        return dimensions.map { dimension ->
            val columnMeta = tableMeta.columns.find { it.name == dimension.name }
            dimension.copy(fieldType = dimension.fieldType ?: columnMeta?.type)
        }
    }

    fun discoverMappingDimensions(dimensions: List<Model.Dimension>): Model.MappingDimensions {
        val mappings = Model.MappingDimensions()

        val sortedDimensions = dimensions.sortedBy { it.name }
        Model.MappingDimensions.CommonMappings.values().forEach { dim ->
            val allAvailableDimensions = sortedDimensions.filter { dimension -> dimension.fieldType == dim.fieldType }
            val fittingDimension = allAvailableDimensions.find { dim.possibleNames.contains(it.name) }
            if (fittingDimension != null) {
                mappings.put(dim, fittingDimension.name)
            } else {
                if (dim == EVENT_TIMESTAMP && allAvailableDimensions.size == 1) {
                    mappings.put(dim, allAvailableDimensions[0].name)
                }
            }
        }

        return mappings
    }

    companion object {
        fun fillDefaultPostOperations(dimension: Model.Dimension, metriqlBridge: WarehouseMetriqlBridge): List<String>? {
            return if (dimension.fieldType != null && dimension.postOperations?.isEmpty() == true) {
                when (dimension.fieldType) {
                    FieldType.TIMESTAMP -> {
                        val defaultTimestampPostOperations = metriqlBridge.timeframes.timestampPostOperations
                        defaultTimestampPostOperations.keys.map { it.serializableName }
                    }
                    FieldType.DATE -> {
                        val defaultDatePostOperations = metriqlBridge.timeframes.datePostOperations
                        defaultDatePostOperations.keys.map { it.serializableName }
                    }
                    FieldType.TIME -> {
                        val defaultTimePostOperations = metriqlBridge.timeframes.timePostOperations
                        defaultTimePostOperations.keys.map { it.serializableName }
                    }
                    // TODO: Add rest if when new post-operations implemented.
                    else -> listOf()
                }
            } else {
                dimension.postOperations
            }
        }

        private val modelReplaceRegex = "[^a-z_][^a-z0-9_]*".toRegex()
        private fun toMetriqlConventionalName(name: String): String {
            val preProcessed = unidecode(name)
                .trim()
                .replace(" ", "_")
                .toLowerCase()
            return preProcessed.replace(modelReplaceRegex, "_").take(120)
        }

        fun createDimensionsFromColumns(columns: List<TableSchema.Column>): List<Model.Dimension> {
            return columns.map {
                val type = if (it.sql == null) Model.Dimension.Type.COLUMN else Model.Dimension.Type.SQL
                val value = if (it.sql == null) Model.Dimension.DimensionValue.Column(it.name) else Model.Dimension.DimensionValue.Sql(it.sql)
                val dimensionName = toMetriqlConventionalName(it.name)
                Model.Dimension(
                    dimensionName,
                    type,
                    value,
                    null,
                    when {
                        it.label != null && it.label.lowercase() != it.name -> it.label
                        dimensionName != it.name.lowercase() -> it.name
                        else -> null
                    },
                    null,
                    null,
                    false,
                    null,
                    null,
                    it.type
                )
            }
        }

        fun createModelFromTable(name: String, tableSchema: TableSchema, target: Model.Target): Model {
            val dimensions = createDimensionsFromColumns(tableSchema.columns)
            return Model(
                name,
                false,
                target,
                null,
                if (tableSchema.comment != null && tableSchema.comment.isNotBlank()) tableSchema.comment else null,
                null,
                discoverMapping(dimensions),
                listOf(),
                dimensions,
                listOf(),
                listOf()
            )
        }

        private fun discoverMapping(dimensions: List<Model.Dimension>): Model.MappingDimensions {
            val userId = dimensions.find { it.name.contains("user") && it.fieldType == FieldType.STRING }
            val eventTimestamp = dimensions.find {
                it.name.contains("time") && it.fieldType == FieldType.TIMESTAMP ||
                    it.name.contains("date") && it.fieldType == FieldType.DATE
            }
            val deviceId = dimensions.find {
                it.name.contains("device") && it.fieldType == FieldType.STRING
            }

            val mappings = Model.MappingDimensions()
            eventTimestamp?.name?.let { mappings.put(EVENT_TIMESTAMP, it) }
            userId?.name?.let { mappings.put(USER_ID, it) }
            deviceId?.name?.let { mappings.put(DEVICE_ID, it) }

            return mappings
        }
    }
}
