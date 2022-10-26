package com.metriql.warehouse.spi.services.retention

import com.metriql.report.retention.RetentionQuery
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.services.ServiceQueryDSL
import com.metriql.warehouse.spi.services.ServiceQueryGenerator
import com.metriql.warehouse.spi.services.ServiceSupport
import io.netty.handler.codec.http.HttpResponseStatus

interface RetentionQueryGenerator : ServiceQueryGenerator<Retention, RetentionQuery, RetentionSupport> {
    fun checkSupport(approximate: Boolean, options: RetentionQuery) {
        val support = supports().firstOrNull { it.approximate == approximate }
            ?: throw MetriqlException("Retention approximation type is not supported", HttpResponseStatus.BAD_REQUEST)
        if (!support.dimension && options.dimension != null) {
            throw MetriqlException("Retention Dimension is not supported.", HttpResponseStatus.BAD_REQUEST)
        }
    }
}

data class Retention(
    val firstStep: Step,
    val returningStep: Step,
    val dateUnit: String,
) : ServiceQueryDSL {
    data class Step(
        val model: String,
        val connector: String,
        val eventTimestamp: String,
        val filters: List<String>?,
        val dimension: String?,
    )
}

data class RetentionSupport(val approximate: Boolean, val dimension: Boolean) : ServiceSupport
