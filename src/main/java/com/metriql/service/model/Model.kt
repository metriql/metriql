package com.metriql.service.model

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.base.CaseFormat
import com.metriql.db.FieldType
import com.metriql.db.JSONBSerializable
import com.metriql.report.ReportType
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.recipe.OrFilters
import com.metriql.report.segmentation.SegmentationRecipeQuery.SegmentationMaterialize
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.HashMap
import kotlin.reflect.KClass

typealias DimensionName = String

fun DimensionName.getMappingDimensionIfValid() = if (this.startsWith(":"))
    JsonHelper.convert(this.substring(1), Model.MappingDimensions.CommonMappings::class.java)
else null

typealias MeasureName = String
typealias ModelName = String
typealias RelationName = String

data class ModelDimension(val modelName: String, val target: Model.Target, val dimension: Model.Dimension)
data class ModelMeasure(val modelName: String, val target: Model.Target, val measure: Model.Measure, val extraFilters: List<ReportFilter>? = null)
data class ModelRelation(
    val sourceModelTarget: Model.Target,
    val sourceModelName: ModelName,
    val targetModelTarget: Model.Target,
    val targetModelName: ModelName,
    val relation: Model.Relation,
)

data class Model(
    val name: String,
    val hidden: Boolean,
    val target: Target,
    val label: String?,
    val description: String?,
    val category: String?,
    val mappings: MappingDimensions,
    val relations: List<Relation>,
    val dimensions: List<Dimension>,
    val measures: List<Measure>,
    val materializes: List<Materialize>? = null,
    val alwaysFilters: List<OrFilters>? = null,
    val id: Int? = null,
    val tags: List<String>? = null,
    val recipeId: Int? = null,
    val recipePath: String? = null,
) {

    data class Materialize(val name: String, val reportType: ReportType, val value: SegmentationMaterialize)

    @JSONBSerializable
    data class Target(
        val type: Type,
        @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
        val value: TargetValue,
    ) {
        @UppercaseEnum
        enum class Type(private val clazz: KClass<out TargetValue>) : StrValueEnum {
            TABLE(TargetValue.Table::class),
            SQL(TargetValue.Sql::class);

            override fun getValueClass() = clazz.java
        }

        @JsonIgnore
        fun needsWith(): Boolean {
            return when (value) {
                is TargetValue.Table -> {
                    false
                }
                is TargetValue.Sql -> {
                    value.sql.contains(' ')
                }
            }
        }

        sealed class TargetValue {
            data class Table(val database: String?, val schema: String?, val table: String) : TargetValue()

            @JsonIgnoreProperties(value = ["dbt"])
            data class Sql(val sql: String) : TargetValue()
        }

        companion object {
            fun initWithTable(database: String?, schema: String?, table: String): Target {
                return Target(Type.TABLE, TargetValue.Table(database, schema, table))
            }
        }
    }

    class MappingDimensions : HashMap<String, DimensionName?>() {
        fun get(type: CommonMappings): DimensionName? {
            return get(JsonHelper.convert(type, String::class.java))
        }

        fun put(type: CommonMappings, value: String): DimensionName? {
            return put(JsonHelper.convert(type, String::class.java), value)
        }

        override fun put(key: String, value: DimensionName?): DimensionName? {
            val finalKey = if (key.chars().anyMatch { Character.isUpperCase(it) }) {
                key
            } else {
                CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key)
            }
            return super.put(finalKey, value)
        }

        @UppercaseEnum
        enum class CommonMappings(val fieldType: FieldType, val possibleNames: List<String>, val discoveredMeasure: (DimensionName) -> Measure?) {
            EVENT_TIMESTAMP(FieldType.TIMESTAMP, listOf("_time", "timestamp"), { null }),
            INCREMENTAL(FieldType.TIMESTAMP, listOf("server_time", "_server_time"), { null }),
            USER_ID(
                FieldType.STRING, listOf("_user", "user", "user_id"),
                {
                    Measure(
                        "unique_users",
                        null,
                        null,
                        null,
                        Measure.Type.DIMENSION,
                        Measure.MeasureValue.Column(Measure.AggregationType.COUNT_UNIQUE, it),
                        null, null
                    )
                }
            ),
            DEVICE_ID(
                FieldType.STRING, listOf("device_id"),
                {
                    Measure(
                        "unique_devices",
                        null,
                        null,
                        null,
                        Measure.Type.DIMENSION,
                        Measure.MeasureValue.Column(Measure.AggregationType.COUNT_UNIQUE, it),
                        null, null
                    )
                }
            ),
            SESSION_ID(
                FieldType.STRING, listOf("session_id"),
                {
                    Measure(
                        "unique_sessions",
                        null,
                        null,
                        null,
                        Measure.Type.DIMENSION,
                        Measure.MeasureValue.Column(Measure.AggregationType.COUNT_UNIQUE, it),
                        null, null
                    )
                }
            );
        }

        companion object {
            fun build(vararg mappings: Pair<CommonMappings, String?>?): MappingDimensions {
                val mapping = MappingDimensions()
                mappings?.forEach {
                    if (it?.second != null) {
                        mapping.put(it.first, it.second!!)
                    }
                }
                return mapping
            }
        }
    }

    data class Dimension(
        val name: String,
        val type: Type,
        @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
        val value: DimensionValue,
        val description: String? = null,
        val label: String? = null,
        val category: String? = null,
        val primary: Boolean? = null,
        val pivot: Boolean? = null,
        val suggestions: List<String>? = null,
        val postOperations: List<String>? = null,
        val fieldType: FieldType? = null,
        val reportOptions: ObjectNode? = null,
        val hidden: Boolean? = null,
        val tags: List<String>? = null
    ) {
        @UppercaseEnum
        enum class Type(private val clazz: KClass<out DimensionValue>) : StrValueEnum {
            COLUMN(DimensionValue.Column::class),
            SQL(DimensionValue.Sql::class);

            override fun getValueClass() = clazz.java
        }

        @JSONBSerializable
        sealed class DimensionValue {
            data class Column(val column: String) : DimensionValue()
            data class Sql(val sql: String, val window: Boolean? = null) : DimensionValue()
        }
    }

    data class Measure(
        val name: String,
        val label: String?,
        val description: String?,
        val category: String?,
        val type: Type,
        @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
        val value: MeasureValue,
        val filters: List<ReportFilter>? = null,
        val reportOptions: ObjectNode? = null,
        val fieldType: FieldType? = FieldType.DOUBLE,
        val hidden: Boolean? = null,
        val tags: List<String>? = null
    ) {

        init {
            // Validate measure filters
            filters?.forEach {
                if (it.value is ReportFilter.FilterValue.MetricFilter && it.value.metricType == ReportFilter.FilterValue.MetricFilter.MetricType.MEASURE) {
                    throw MetriqlException("Only dimension filters are supported inside filters for measure: $name", HttpResponseStatus.BAD_REQUEST)
                }
            }
        }

        @UppercaseEnum
        enum class Type(private val clazz: KClass<out MeasureValue>) : StrValueEnum {
            COLUMN(MeasureValue.Column::class), // This is not dimension, raw column
            DIMENSION(MeasureValue.Dimension::class), // This is not dimension, raw column
            SQL(MeasureValue.Sql::class);

            override fun getValueClass(): Class<*> {
                return clazz.java
            }
        }

        @JSONBSerializable
        sealed class MeasureValue(@JsonIgnore val agg: AggregationType?) {
            data class Column(val aggregation: AggregationType, val column: String?) : MeasureValue(aggregation)
            data class Dimension(val aggregation: AggregationType, val dimension: String?) : MeasureValue(aggregation)
            data class Sql(val sql: String, val aggregation: AggregationType?, val window: Boolean? = null) : MeasureValue(aggregation)
        }

        @UppercaseEnum
        enum class AggregationType {
            COUNT, COUNT_UNIQUE, SUM, MINIMUM, MAXIMUM, AVERAGE, APPROXIMATE_UNIQUE, SUM_DISTINCT, AVERAGE_DISTINCT, SQL

        }
    }

    data class Relation(
        val name: String,
        val label: String?,
        val description: String?,
        val relationType: RelationType,
        @JsonAlias("join")
        val joinType: JoinType = JoinType.LEFT_JOIN,
        val modelName: ModelName,
        val type: Type,
        @PolymorphicTypeStr<Type>(externalProperty = "type", valuesEnum = Type::class)
        val value: RelationValue,
        val hidden: Boolean? = null,
        val fields: Set<String>? = null,
    ) {
        @UppercaseEnum
        enum class Type(private val clazz: KClass<out RelationValue>) : StrValueEnum {
            DIMENSION(RelationValue.DimensionValue::class),
            COLUMN(RelationValue.ColumnValue::class), // This is not dimension, raw column
            SQL(RelationValue.SqlValue::class);

            override fun getValueClass() = clazz.java
        }

        @UppercaseEnum
        enum class JoinType {
            INNER_JOIN,
            LEFT_JOIN,
            RIGHT_JOIN,
            FULL_JOIN;
        }

        @UppercaseEnum
        enum class RelationType {
            ONE_TO_ONE,
            ONE_TO_MANY,
            MANY_TO_ONE,
            MANY_TO_MANY;
        }

        @JSONBSerializable
        sealed class RelationValue {
            data class DimensionValue(val sourceDimension: String, val targetDimension: String) : RelationValue()
            data class SqlValue(val sql: String) : RelationValue()
            data class ColumnValue(val sourceColumn: String, val targetColumn: String) : RelationValue()
        }
    }
}
