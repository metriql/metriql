package com.metriql.service.jinja

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.hubspot.jinjava.el.ext.NamedParameter
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.metriql.report.RecipeQueryJsonDeserializer
import com.metriql.report.ReportType
import com.metriql.service.auth.ProjectAuth
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.RecipeQuery
import io.netty.handler.codec.http.HttpResponseStatus

object Functions {
    @JvmStatic
    fun sql(vararg arguments: Any?): String? {
        val ctx = JinjavaInterpreter.getCurrent().context
        val context = ctx["_context"] as IQueryGeneratorContext
        val auth = ctx["_auth"] as ProjectAuth

        val type = arguments[0].toString()
        val options = arguments.mapNotNull {
            when (it) {
                is NamedParameter -> it
                else -> null
            }
        }.map { it.name to it.value }.toMap()

        val pair = JsonHelper.getMapper().convertValue(mapOf("type" to type, "options" to options), ReportPair::class.java)

        val executor = context.reportExecutor ?: throw MetriqlException("Report executor is not available in this context", HttpResponseStatus.BAD_REQUEST)
        return executor.invoke(auth, pair.type, pair.options)
    }

    data class ReportPair(
        val type: ReportType,
        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type", defaultImpl = RecipeQueryJsonDeserializer::class)
        val options: RecipeQuery
    )
}
