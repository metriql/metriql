package com.metriql.report

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.metriql.service.model.DimensionName
import com.metriql.service.model.MeasureName
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName
import com.metriql.service.model.RelationName
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

// legacy properties
@JsonIgnoreProperties("metricName", "metricModelName", "metricRelationName")
sealed class ReportMetric {
    data class ReportMappingDimension(
        val name: Model.MappingDimensions.CommonMappings,
        val postOperation: PostOperation?
    ) : ReportMetric() {
        override fun toMetricReference() = Recipe.MetricReference.mappingDimension(name, null)
    }

    data class Function(val name: SqlFunction, val parameters: List<ReportMetric>) : ReportMetric() {
        enum class SqlFunction(aggregation: Boolean) {
            COUNT(true)
        }

        override fun toMetricReference(): Recipe.MetricReference {
            TODO("not implemented")
        }
    }

    data class Unary(val operator: Operator, val left: ReportMetric, val right: ReportMetric) : ReportMetric() {
        enum class Operator(name: String) {
            PLUS("+"), MINUS("-")
        }

        override fun toMetricReference(): Recipe.MetricReference {
            TODO("not implemented")
        }
    }

    data class ReportDimension(
        @JsonAlias("dimension")
        val name: DimensionName,
        // for dashboard filters compatibility
        @JsonAlias("model")
        val modelName: ModelName?,
        val relationName: RelationName?,
        val postOperation: PostOperation?,
        val pivot: Boolean? = null
    ) : ReportMetric() {
        init {
            if (modelName == null && relationName == null) {
                throw MetriqlException("Cant infer measures model if both modelName and relationName not set", HttpResponseStatus.BAD_REQUEST)
            }
        }

        override fun toMetricReference(): Recipe.MetricReference = Recipe.MetricReference(name, relationName)

        fun toReference(): Recipe.DimensionReference {
            return Recipe.DimensionReference(Recipe.MetricReference(name, relationName), postOperation?.value?.name?.toLowerCase())
        }
    }

    data class ReportMeasure(
        val modelName: ModelName,
        val name: MeasureName,
        val relationName: RelationName? = null,
    ) : ReportMetric() {
        init {
            if (modelName == null && relationName == null) {
                throw MetriqlException("Cant infer measures model if both modelName and relationName not set", HttpResponseStatus.BAD_REQUEST)
            }
        }

        override fun toMetricReference(): Recipe.MetricReference {
            return Recipe.MetricReference(name, relationName)
        }
    }

    data class PostOperation(
        val type: Type, // Type is the same with `FieldType`. For simplicity; duplicated
        @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
        val value: Enum<*>
    ) {
        @UppercaseEnum
        enum class Type(val clazz: KClass<out Enum<*>>) : StrValueEnum {
            TIMESTAMP(TimestampPostOperation::class),
            DATE(DatePostOperation::class),
            TIME(TimePostOperation::class);

            override fun getValueClass() = clazz.java
        }

        fun toFieldType(): com.metriql.db.FieldType {
            return when (type) {
                Type.TIME -> com.metriql.db.FieldType.TIME
                Type.TIMESTAMP -> com.metriql.db.FieldType.TIMESTAMP
                Type.DATE -> com.metriql.db.FieldType.DATE
            }
        }

        companion object {
            fun fromFieldType(type: com.metriql.db.FieldType, name: String): PostOperation {
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

                return PostOperation(value, JsonHelper.convert(name, value.clazz.java))
            }
        }
    }

    abstract fun toMetricReference(): Recipe.MetricReference
}
