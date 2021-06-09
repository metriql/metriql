package io.trino

import com.metriql.db.FieldType
import com.metriql.service.model.Model
import io.trino.connector.system.SystemTablesProvider
import io.trino.spi.connector.ColumnMetadata
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTableMetadata
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.connector.SystemTable
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.TimeType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType
import io.trino.type.UnknownType
import java.util.Optional

class MetriqlMetadata(val models: List<Model>) : SystemTablesProvider {

    override fun listSystemTables(session: ConnectorSession?): Set<SystemTable> {
        return models.map { ModelProxy(models, it) }.toSet()
    }

    override fun getSystemTable(session: ConnectorSession?, tableName: SchemaTableName): Optional<SystemTable> {
        val model = models.find { it.category == getCategoryFromSchema(tableName.schemaName) && it.name == tableName.tableName } ?: return Optional.empty()
        return Optional.of(ModelProxy(models, model))
    }

    private fun getCategoryFromSchema(schemaName: String): Any? {
        return if (schemaName == "public") null else schemaName;
    }

    class ModelProxy(val models: List<Model>, val model: Model) : SystemTable {
        override fun getDistribution() = SystemTable.Distribution.SINGLE_COORDINATOR

        private fun toColumnMetadata(it: Model.Measure, relation: String? = null): ColumnMetadata? {
            if (it.value.agg != null) {
                return null
            }

            return ColumnMetadata.builder()
                .setName(relation?.let { _ -> "$relation.${it.name}" } ?: it.name)
                .setComment(Optional.ofNullable(it.description))
                .setHidden(it.hidden ?: false)
                .setNullable(true)
                .setType(getTrinoType(it.fieldType))
                .build()
        }

        private fun toColumnMetadata(it: Model.Dimension, relation: String? = null): ColumnMetadata {
            return ColumnMetadata.builder()
                .setName(relation?.let { _ -> "$relation.${it.name}" } ?: it.name)
                .setComment(Optional.ofNullable(it.description))
                .setHidden(it.hidden ?: false)
                .setNullable(true)
                .setType(getTrinoType(it.fieldType))
                .build()
        }

        override fun getTableMetadata(): ConnectorTableMetadata {
            val columns = mutableListOf<ColumnMetadata>()
            model.dimensions.forEach { columns.add(toColumnMetadata(it)) }
            model.measures.filter { it.value.agg != null }.mapNotNull { toColumnMetadata(it) }.forEach { columns.add(it) }

            model.relations?.forEach { relation ->
                models.find { it.name == relation.modelName }?.let { model ->
                    model.dimensions.forEach { columns.add(toColumnMetadata(it, relation.name)) }
                    model.measures.filter { it.value.agg != null }.mapNotNull { toColumnMetadata(it, relation.name) }.forEach { columns.add(it) }
                }
            }

            return ConnectorTableMetadata(
                SchemaTableName(model.category ?: "public", model.name),
                columns,
                mapOf("label" to model.label),
                Optional.ofNullable(model.description)
            )
        }
    }

    companion object {
        private val decimalType = DecimalType.createDecimalType()

        fun getTrinoType(type: FieldType?): Type {
            return when (type) {
                FieldType.INTEGER -> IntegerType.INTEGER
                FieldType.STRING -> VarcharType.VARCHAR
                FieldType.DECIMAL -> decimalType
                FieldType.DOUBLE -> DoubleType.DOUBLE
                FieldType.LONG -> BigintType.BIGINT
                FieldType.BOOLEAN -> BooleanType.BOOLEAN
                FieldType.DATE -> DateType.DATE
                FieldType.TIME -> TimeType.TIME
                FieldType.TIMESTAMP -> TimestampType.TIMESTAMP
                FieldType.BINARY -> TODO()
                FieldType.ARRAY_STRING -> TODO()
                FieldType.ARRAY_INTEGER -> TODO()
                FieldType.ARRAY_DOUBLE -> TODO()
                FieldType.ARRAY_LONG -> TODO()
                FieldType.ARRAY_BOOLEAN -> TODO()
                FieldType.ARRAY_DATE -> TODO()
                FieldType.ARRAY_TIME -> TODO()
                FieldType.ARRAY_TIMESTAMP -> TODO()
                FieldType.MAP_STRING -> TODO()
                FieldType.UNKNOWN, null -> UnknownType.UNKNOWN
            }
        }

        fun getMetriqlType(type: Type): FieldType {
            val name = type.baseName


            return when {
                IntegerType.INTEGER.baseName == name -> FieldType.INTEGER
                VarcharType.VARCHAR.baseName == name -> FieldType.STRING
                decimalType.baseName == name -> FieldType.DECIMAL
                DoubleType.DOUBLE.baseName == name -> FieldType.DOUBLE
                BigintType.BIGINT.baseName == name -> FieldType.LONG
                BooleanType.BOOLEAN.baseName == name -> FieldType.BOOLEAN
                DateType.DATE.baseName == name -> FieldType.DATE
                TimeType.TIME.baseName == name -> FieldType.TIME
                TimestampType.TIMESTAMP.baseName == name -> FieldType.TIMESTAMP
                type.baseName == "array" -> TODO()
                type.baseName == "map" -> TODO()
                else -> TODO()
            }
        }
    }
}
