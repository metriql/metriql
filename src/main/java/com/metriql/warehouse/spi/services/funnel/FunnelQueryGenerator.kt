package com.metriql.warehouse.spi.services.funnel

import com.metriql.report.funnel.FunnelQuery
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.services.ServiceQueryDSL
import com.metriql.warehouse.spi.services.ServiceQueryGenerator
import com.metriql.warehouse.spi.services.ServiceSupport
import io.netty.handler.codec.http.HttpResponseStatus

interface FunnelQueryGenerator : ServiceQueryGenerator<Funnel, FunnelQuery, FunnelSupport> {
    fun checkSupport(isStrictlyOrdered: Boolean, options: FunnelQuery) {
        val support = supports().firstOrNull { it.isStrictlyOrdered == isStrictlyOrdered }
            ?: throw MetriqlException("Funnel order type is not supported", HttpResponseStatus.BAD_REQUEST)
        if (!support.approximation && options.approximate) {
            throw MetriqlException("Funnel Approximation is not supported.", HttpResponseStatus.BAD_REQUEST)
        }
        if (!support.dimension && options.dimension != null) {
            throw MetriqlException("Funnel dimension is not supported.", HttpResponseStatus.BAD_REQUEST)
        }
        if (!support.exclusion && !options.excludedSteps.isNullOrEmpty()) {
            throw MetriqlException("Funnel Exclusion steps are not supported.", HttpResponseStatus.BAD_REQUEST)
        }
        if (!support.window && options.window != null) {
            throw MetriqlException("Funnel window is not support", HttpResponseStatus.BAD_REQUEST)
        }
    }
}

data class Funnel(
    val steps: List<Step>,
    val excludedSteps: List<ExcludedStep>?,
    val hasDimension: Boolean,
    val windowInSeconds: Long?,
    val sorting: String,
) : ServiceQueryDSL {
    data class Step(
        val index: Int,
        val model: String,
        val connector: String,
        val eventTimestamp: String,
        val filters: List<String>?,
        val dimension: String?,
    )

    data class ExcludedStep(
        val step: Step,
        val start: Int?,
    )
}

data class FunnelSupport(val isStrictlyOrdered: Boolean, val approximation: Boolean, val dimension: Boolean, val window: Boolean, val exclusion: Boolean) : ServiceSupport
