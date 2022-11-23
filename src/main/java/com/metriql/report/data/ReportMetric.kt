package com.metriql.report.data

import com.metriql.db.FieldType
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.DatasetName
import com.metriql.service.dataset.DimensionName
import com.metriql.service.dataset.MeasureName
import com.metriql.service.dataset.RelationName
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import io.netty.handler.codec.http.HttpResponseStatus
import java.lang.IllegalArgumentException
import kotlin.reflect.KClass

sealed class ReportMetric {
    data class ReportMappingDimension(
        val name: Dataset.MappingDimensions.CommonMappings,
        val timeframe: Timeframe?
    ) : ReportMetric() {
        override fun toMetricReference() = Recipe.FieldReference.mappingDimension(name, null)
    }

    data class ReportDimension(
        val name: DimensionName,
        val dataset: DatasetName,
        val relation: RelationName?,
        val timeframe: Timeframe?,
    ) : ReportMetric() {
        init {
            if (dataset == null && relation == null) {
                throw MetriqlException("Cant infer measures model if both modelName and relationName not set", HttpResponseStatus.BAD_REQUEST)
            }
        }

        override fun toMetricReference(): Recipe.FieldReference = Recipe.FieldReference(name, relation)

        fun toReference(): Recipe.FieldReference {
            return Recipe.FieldReference(name, relation, timeframe?.value?.name?.lowercase())
        }
    }

    data class ReportMeasure(
        val dataset: DatasetName,
        val name: MeasureName,
        val relation: RelationName? = null,
    ) : ReportMetric() {
        init {
            if (dataset == null && relation == null) {
                throw MetriqlException("Cant infer measures model if both modelName and relationName not set", HttpResponseStatus.BAD_REQUEST)
            }
        }

        override fun toMetricReference(): Recipe.FieldReference {
            return Recipe.FieldReference(name, relation)
        }
    }

    data class Timeframe(
        val type: Type, // Type is the same with `FieldType`. For simplicity; duplicated
        @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
        val value: Enum<*>
    ) {
        @UppercaseEnum
        enum class Type(val clazz: KClass<out Enum<*>>, val fieldType: FieldType) : StrValueEnum {
            TIMESTAMP(TimestampPostOperation::class, FieldType.TIMESTAMP),
            DATE(DatePostOperation::class, FieldType.DATE),
            TIME(TimePostOperation::class, FieldType.TIME);

            override fun getValueClass() = clazz.java
        }

        companion object {
            fun fromFieldType(type: com.metriql.db.FieldType, name: String): Timeframe {
                val value = when (type) {
                    com.metriql.db.FieldType.TIME -> Type.TIME
                    com.metriql.db.FieldType.TIMESTAMP -> Type.TIMESTAMP
                    com.metriql.db.FieldType.DATE -> Type.DATE
                    com.metriql.db.FieldType.UNKNOWN -> {
                        throw IllegalArgumentException("Post operation can only be used when the `type` is defined")
                    }
                    else -> {
                        throw IllegalArgumentException("{${type.name} type does not have $name operation}")
                    }
                }

                return Timeframe(value, JsonHelper.convert(name, value.clazz.java))
            }
        }
    }

    abstract fun toMetricReference(): Recipe.FieldReference
}
