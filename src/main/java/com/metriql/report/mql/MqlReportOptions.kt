package com.metriql.report.mql

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.trino.sql.tree.Expression

@JsonIgnoreProperties(value = ["version"])
data class MqlReportOptions(
    val query: String,
    val queryOptions: QueryOptions?,
    val variables: List<Expression>?,
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

    data class QueryOptions(val limit: Int?, val defaultDatabase: String?, val defaultSchema: String?, val useCache: Boolean = true)
}
