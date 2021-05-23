package com.metriql.report.sql

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions
import kotlin.reflect.KClass

@JsonIgnoreProperties(value = ["version"])
data class SqlReportOptions(
    val query: String,
    val queryOptions: QueryOptions?,
    val variables: List<Variable<*>>?,
    val reportOptions: ReportOptions?
) : ServiceReportOptions, RecipeQuery {
    override fun toRecipeQuery() = this
    override fun toReportOptions(context: IQueryGeneratorContext) = this

    override fun getQueryLimit(): Int? {
        return queryOptions?.limit
    }

    @JsonIgnoreProperties(value = ["segmentedColumnOptions"])
    data class ReportOptions(
        val measures: List<Int>?,
        val dimensions: List<Int>?,
        val pivots: List<Int>?,
        val chartOptions: ObjectNode?,
        val tableOptions: ObjectNode?,
        val columnOptions: List<ObjectNode>?,
        val enableStatistics: Boolean?
    )

    data class Variable<out T>(
        val name: String,
        val type: VariableType,
        val label: String?,
        val defaultValue: Any?,
        @PolymorphicTypeStr<VariableType>(externalProperty = "type", valuesEnum = VariableType::class)
        val typeOptions: T?
    ) {
        @UppercaseEnum
        enum class VariableType(val clazz: KClass<*>) : StrValueEnum {
            STRING(StringVariableOptions::class), DATE(Nothing::class), DATE_RANGE(Nothing::class);

            data class StringVariableOptions(val value: Any?, val allowUserInput: Boolean)

            override fun getValueClass() = clazz.java
        }
    }

    data class QueryOptions(val limit: Int?, val defaultDatabase: String?, val defaultSchema: String?, val useCache: Boolean = true)
}
