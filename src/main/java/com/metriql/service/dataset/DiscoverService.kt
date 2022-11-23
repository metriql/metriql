package com.metriql.service.dataset

import com.metriql.db.FieldType
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.TIME_SERIES
import com.metriql.service.dataset.Dataset.MappingDimensions.CommonMappings.USER_ID
import com.metriql.util.MetriqlException
import com.metriql.util.TextUtil.toMetriqlConventionalName
import com.metriql.util.serializableName
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.TableSchema
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.logging.Level
import java.util.logging.Logger

class DiscoverService(private val dataSource: DataSource) {

    fun discoverMeasuresFromScratch(dimensions: List<Dataset.Dimension>, mappings: Dataset.MappingDimensions): List<Dataset.Measure> {
        // This default applies for all
        val measures = mutableListOf<Dataset.Measure>()
        measures.add(
            Dataset.Measure(
                "count_of_rows", null, null, null, Dataset.Measure.Type.COLUMN, Dataset.Measure.MeasureValue.Column(Dataset.Measure.AggregationType.COUNT, null), null, null
            )
        )

        Dataset.MappingDimensions.CommonMappings.values().mapNotNull { mapping -> mappings.get(mapping)?.let { mapping.discoveredMeasure.invoke(it) } }.forEach {
            measures.add(it)
        }

        // Add numeric dimensions of SUM(col)
        dimensions.filter { it.type == Dataset.Dimension.Type.COLUMN }.filter { it.fieldType?.isNumeric == true }.forEach {
            measures.add(
                Dataset.Measure(
                    toMetriqlConventionalName("sum_of_${it.name}"),
                    null,
                    it.description,
                    null,
                    Dataset.Measure.Type.DIMENSION,
                    Dataset.Measure.MeasureValue.Dimension(Dataset.Measure.AggregationType.SUM, it.name),
                    null,
                    null
                )
            )
        }

        return measures
    }

    fun discoverDimensionFieldTypes(
        context: IQueryGeneratorContext,
        modelName: String,
        datasetTarget: Dataset.Target,
        dimensions: List<Dataset.Dimension>
    ): List<Dataset.Dimension> {
        val dimensionsWithoutFieldType = dimensions.filter { it.fieldType == null && it.hidden != true }

        if (dimensionsWithoutFieldType.isEmpty()) return dimensions

        val query = dataSource.warehouse.bridge.generateDimensionMetaQuery(
            context, modelName, datasetTarget, dimensionsWithoutFieldType
        )

        logger.info("Discovering ${dimensionsWithoutFieldType.size} dimensions of model `$modelName`")
        val tableMeta = try {
            dataSource.getTableSchema(query)
        } catch (e: Exception) {
            logger.log(Level.WARNING, "Unable to discover dimension types", e)
            throw MetriqlException("Failed to run query for exploring the dimension types: ${e.message} \n $query", HttpResponseStatus.BAD_REQUEST)
        }

        return dimensions.map { dimension ->
            val columnMeta = tableMeta.columns.find { it.name.equals(dimension.name, ignoreCase = true) }
            dimension.copy(fieldType = dimension.fieldType ?: columnMeta?.type)
        }
    }

    fun discoverMappingDimensions(dimensions: List<Dataset.Dimension>): Dataset.MappingDimensions {
        val mappings = Dataset.MappingDimensions()

        val sortedDimensions = dimensions.sortedBy { it.name }
        Dataset.MappingDimensions.CommonMappings.values().forEach { dim ->
            val allAvailableDimensions = sortedDimensions.filter { dimension -> dimension.fieldType == dim.fieldType }
            val fittingDimension = allAvailableDimensions.find { dim.possibleNames.contains(it.name) }
            if (fittingDimension != null) {
                mappings.put(dim, fittingDimension.name)
            } else {
                if (dim == TIME_SERIES && allAvailableDimensions.size == 1) {
                    mappings.put(dim, allAvailableDimensions[0].name)
                }
            }
        }

        return mappings
    }

    companion object {
        fun fillDefaultPostOperations(dimension: Dataset.Dimension, metriqlBridge: WarehouseMetriqlBridge): List<String>? {
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

        fun createDimensionsFromColumns(columns: List<TableSchema.Column>): List<Dataset.Dimension> {
            return columns.map {
                val type = if (it.sql == null) Dataset.Dimension.Type.COLUMN else Dataset.Dimension.Type.SQL
                val value = if (it.sql == null) Dataset.Dimension.DimensionValue.Column(it.name) else Dataset.Dimension.DimensionValue.Sql(it.sql)
                val dimensionName = toMetriqlConventionalName(it.name)
                Dataset.Dimension(
                    dimensionName, type, value, null,
                    when {
                        it.label != null && it.label.lowercase() != it.name -> it.label
                        dimensionName != it.name.lowercase() -> it.name
                        else -> null
                    },
                    null, false, null, null, it.type
                )
            }
        }

        fun createModelFromTable(name: String, tableSchema: TableSchema, target: Dataset.Target): Dataset {
            val dimensions = createDimensionsFromColumns(tableSchema.columns)
            return Dataset(
                name,
                false,
                target,
                null,
                if (!tableSchema.comment.isNullOrBlank()) tableSchema.comment else null,
                null,
                discoverMapping(dimensions),
                listOf(),
                dimensions,
                listOf()
            )
        }

        private fun discoverMapping(dimensions: List<Dataset.Dimension>): Dataset.MappingDimensions {
            val userId = dimensions.find { it.name.contains("user") && it.fieldType == FieldType.STRING }
            val eventTimestamp = dimensions.find {
                it.name.contains("time") && it.fieldType == FieldType.TIMESTAMP || it.name.contains("date") && it.fieldType == FieldType.DATE
            }

            val mappings = Dataset.MappingDimensions()
            eventTimestamp?.name?.let { mappings.put(TIME_SERIES, it) }
            userId?.name?.let { mappings.put(USER_ID, it) }

            return mappings
        }

        private val logger = Logger.getLogger(this::class.java.name)
    }
}
