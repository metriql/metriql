package com.metriql.report.data.recipe

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.metriql.report.ReportType
import com.metriql.report.data.Dashboard
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.service.dataset.Dataset
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus

data class RecipeDashboard(
    val name: String,
    val description: String? = null,
    val category: String? = null,
    @JsonAlias("filterSchema")
    val filters: Map<String, RecipeFilterSchema>? = null,
    val reports: List<Reports?>,
    @JsonIgnore
    val _path: String?,
) {
    data class RecipeFilterSchema(
        val model: String?,
        val dimension: Recipe.FieldReference?,
        val mappingDimension: Recipe.FieldReference?,
        @JsonAlias("operation")
        val timeframe: String?,
        val default: Any?,
        val required: Boolean?,
    ) {

        fun toDashboardFilterSchemaItem(
            context: IQueryGeneratorContext,
            name: String,
        ): Dashboard.DashboardFilterSchemaItem {
            val (type, value) = when {
                dimension != null -> {
                    val modelName = model ?: throw MetriqlException(
                        "Model is required in `$name` for dimension filter",
                        HttpResponseStatus.BAD_REQUEST
                    )
                    val dim = dimension.toDimension(modelName, dimension.getType(context, modelName))
                    Pair(
                        ReportFilter.FilterValue.MetricFilter.MetricType.DIMENSION,
                        ReportMetric.ReportDimension(dim.name, modelName, null, dim.timeframe)
                    )
                }
                mappingDimension != null -> {
                    Pair(
                        ReportFilter.FilterValue.MetricFilter.MetricType.MAPPING_DIMENSION,
                        ReportMetric.ReportMappingDimension(
                            JsonHelper.convert(
                                mappingDimension.name,
                                Dataset.MappingDimensions.CommonMappings::class.java
                            ),
                            null
                        )
                    )
                }
                else -> {
                    throw MetriqlException(
                        "One of `dimension` or `mappingDimension` must be set in filter `$name`",
                        HttpResponseStatus.BAD_REQUEST
                    )
                }
            }

            return Dashboard.DashboardFilterSchemaItem(name, type, value, default, timeframe, required ?: true)
        }
    }

    @JsonIgnoreProperties(value = ["ttl"])
    data class Reports(
        val name: String,
        val description: String? = null,
        val x: Int? = null,
        val y: Int? = null,
        @JsonAlias("h")
        val height: Int,
        @JsonAlias("w")
        val width: Int,
        val component: String,
        val type: ReportType,
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
        @JsonAlias("reportOptions")
        val options: com.metriql.warehouse.spi.services.RecipeQuery,
    )
}
