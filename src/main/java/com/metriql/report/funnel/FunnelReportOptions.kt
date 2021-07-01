package com.metriql.report.funnel

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.metriql.report.data.Dataset
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.model.DimensionName
import com.metriql.service.model.RelationName
import com.metriql.util.RPeriod
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.WarehouseQueryTask
import com.metriql.warehouse.spi.services.ServiceReportOptions
import java.time.Duration

data class FunnelReportOptions(
    val steps: List<Dataset>,
    val excludedSteps: List<ExcludedStep>?,
    val dimension: FunnelDimension?,
    val window: FunnelWindow?,
    val connector: DimensionName?,
    val strictlyOrdered: Boolean,
    val approximate: Boolean,
    val defaultDateRange: RPeriod? = null
) : ServiceReportOptions {
    override fun toRecipeQuery(): FunnelRecipeQuery {
        return FunnelRecipeQuery(steps.map { it.toRecipe() }, excludedSteps?.map { it.toRecipe() }, dimension?.toRecipe(), window, connector, strictlyOrdered, approximate)
    }

    // If range null then all steps
    data class ExcludedStep(val start: Int?, val step: Dataset) {
        @JsonIgnore
        fun toRecipe(): FunnelRecipeQuery.ExcludedStep {
            return FunnelRecipeQuery.ExcludedStep(start, step.toRecipe())
        }
    }

    data class FunnelDimension(val step: Int, val name: DimensionName, val relationName: RelationName?, val postOperation: ReportMetric.PostOperation?) {
        @JsonIgnore
        fun toRecipe(): FunnelRecipeQuery.FunnelDimension {
            return FunnelRecipeQuery.FunnelDimension(step, Recipe.DimensionReference(Recipe.MetricReference(name, relationName), postOperation?.value?.name))
        }
    }

    @UppercaseEnum
    enum class WindowType(val duration: Duration) {
        SECOND(Duration.ofSeconds(1)),
        MINUTE(Duration.ofMinutes(1)),
        HOUR(Duration.ofHours(1)),
        DAY(Duration.ofDays(1)),
        WEEK(Duration.ofDays(1 * 7));
    }

    @JsonIgnoreProperties(value = ["toSeconds"])
    data class FunnelWindow constructor(val value: Int, val type: WindowType) {
        @JsonIgnore
        fun toSeconds() = value * type.duration.seconds // In use by funnel query template.
    }

    override fun getQueryLimit() = WarehouseQueryTask.MAX_LIMIT
}
