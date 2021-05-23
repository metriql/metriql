package com.metriql.jinja

import com.hubspot.jinjava.el.ext.NamedParameter
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.metriql.auth.ProjectAuth
import com.metriql.report.ReportType
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.PolymorphicTypeStr
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.RecipeQuery
import io.netty.handler.codec.http.HttpResponseStatus

object Functions {
    @JvmStatic
    fun report(vararg arguments: Any?): String? {
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
        return executor.getQuery(auth, pair.type, pair.options)
    }

    data class ReportPair(
        val type: ReportType,
        @PolymorphicTypeStr<ReportType>(externalProperty = "type", valuesEnum = ReportType::class, name = "recipe")
        val options: RecipeQuery
    )
}
