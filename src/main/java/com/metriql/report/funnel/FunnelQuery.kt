package com.metriql.report.funnel

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.metriql.report.data.Dataset
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.dataset.DimensionName
import com.metriql.util.RPeriod
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.services.ServiceQuery
import java.time.Duration

data class FunnelQuery(
    val steps: List<Dataset>,
    val excludedSteps: List<ExcludedStep>?,
    val dimension: FunnelDimension?,
    val window: FunnelWindow?,
    val connector: DimensionName?,
    val strictlyOrdered: Boolean,
    val approximate: Boolean,
    val defaultDateRange: RPeriod? = null
) : ServiceQuery() {

    data class ExcludedStep(val start: Int?, val step: Dataset)
    data class FunnelDimension(val step: Int, val reference: Recipe.FieldReference)

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
}
