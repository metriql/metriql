package com.metriql.report.funnel

import com.fasterxml.jackson.annotation.JsonIgnore
import com.metriql.report.data.RecipeDataset
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.model.DimensionName
import com.metriql.util.RPeriod
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions

data class FunnelRecipeQuery(
    val steps: List<RecipeDataset>,
    val excludedSteps: List<ExcludedStep>?,
    val dimension: FunnelDimension?,
    val window: FunnelReportOptions.FunnelWindow?,
    val connector: DimensionName?,
    val strictlyOrdered: Boolean?,
    val approximate: Boolean?,
    val defaultDateRange: RPeriod? = null
) : RecipeQuery {

    data class ExcludedStep(val start: Int?, val step: RecipeDataset) {
        @JsonIgnore
        fun toDataset(context: IQueryGeneratorContext): FunnelReportOptions.ExcludedStep {
            return FunnelReportOptions.ExcludedStep(start, step.toDataset(context))
        }
    }

    data class FunnelDimension(val step: Int, val dimension: Recipe.FieldReference) {
        @JsonIgnore
        fun toDimension(context: IQueryGeneratorContext, steps: List<RecipeDataset>): FunnelReportOptions.FunnelDimension {
            val postOperation = dimension.timeframe?.let {
                val type = dimension.getType(context, steps[step].dataset)
                ReportMetric.PostOperation.fromFieldType(type, dimension.timeframe)
            }

            return FunnelReportOptions.FunnelDimension(step, dimension.name, dimension.relation, postOperation)
        }
    }

    override fun toReportOptions(context: IQueryGeneratorContext): ServiceReportOptions {
        return FunnelReportOptions(
            steps.map { it.toDataset(context) },
            excludedSteps?.map { it.toDataset(context) },
            dimension?.toDimension(context, steps),
            window,
            connector,
            strictlyOrdered ?: false,
            approximate ?: false
        )
    }
}
