package com.metriql.service.jinja

import com.hubspot.jinjava.Jinjava
import com.hubspot.jinjava.RecursiveJinjava
import com.hubspot.jinjava.lib.fn.ELFunctionDefinition
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import javax.inject.Inject

class JinjaRendererService @Inject constructor() {
    val jinja: Jinjava = RecursiveJinjava()

    init {
        val methods = Functions::class.java.declaredMethods
        methods.forEach {
            jinja.globalContext.registerFunction(ELFunctionDefinition("", it.name, it))
        }

//        jinja.globalContext.registerTag(RealtimeTag())
    }

    fun render(
        auth: ProjectAuth,
        dataSource: DataSource,
        sqlRenderable: SQLRenderable,
        aliasName: String?,
        context: IQueryGeneratorContext,
        inQueryDimensionNames: List<String>? = null,
        dateRange: DateRange? = null,
        sourceModelName: ModelName? = null,
        variables: Map<String, Any> = mapOf(),
        renderAlias: Boolean = false,
        extraContext: Map<String, Any> = mapOf(),
        hook: ((Map<String, Any?>) -> Map<String, Any?>)? = null,
    ): String {
        /*
        * select {{model.firebase_event_in_app_purchase.dimension.user__region_code}}, count(*)
        * from {{model.firebase_event_in_app_purchase}}
        * where {{model.firebase_event_in_app_purchase.dimension.event_timestamp}} between {{date.start}} and {{date.end}}
        * group by 1
        *
        * The query above renders user__region_code dimension on firebase_event_in_app_purchase model. However, that dimension is in SQL context with in_query checking
        * We have to include the dimension names with the same modelname to in_query context so that `firebase_event_in_app_purchase` SQL model
        * will get rendered including that dimension
        * */
        val additionalDimensionsAndColumns = if (sourceModelName != null) {
            context.referencedDimensions.filter { it.key.first == sourceModelName }.map { it.key.second }
        } else {
            listOf()
        }

        val base = extraContext + mapOf(
            "TABLE" to aliasName,
            "aq" to context.warehouseBridge.quote,
            "model" to MetriqlJinjaContext.ModelContext(context, renderAlias),
            "relation" to MetriqlJinjaContext.RelationContext(sourceModelName, context, renderAlias),
            "dimension" to (sourceModelName?.let { MetriqlJinjaContext.DimensionContext(it, null, context, renderAlias) }),
            "measure" to (sourceModelName?.let { MetriqlJinjaContext.MeasureContext(it, null, context, renderAlias) }),
            "user" to MetriqlJinjaContext.UserAttributeContext(context),
            "variables" to variables,
            "_auth" to auth,
            "_context" to context,
        )

        val bindings = base + mapOf(
            "in_query" to MetriqlJinjaContext.InQueryDimensionContext(
                // Include in_query dimensions and additional dimensions from parent context
                ((inQueryDimensionNames ?: listOf()) + additionalDimensionsAndColumns).toSet().toList()
            ),
            "partitioned" to (dateRange != null),
            "date" to dateRange
        )

        return try {
            jinja.render(sqlRenderable, hook?.invoke(bindings) ?: bindings)
        } catch (e: Throwable) {
            throw MetriqlException("Error while rendering jinja expression `$sqlRenderable` for model `$sourceModelName`: ${e.message}", HttpResponseStatus.BAD_REQUEST)
        }
    }

//    class RealtimeTag : Tag {
//        var active = false
//
//        override fun getName(): String {
//            return "realtime"
//        }
//
//        override fun interpret(node: TagNode, interpreter: JinjavaInterpreter): String {
//            return if (interpreter.context["in_query"] != null) {
//                val sb = LengthLimitingStringBuilder(interpreter.config.maxOutputSize)
//                node.children.forEach { sb.append(it.render(interpreter)) }
//                sb.toString()
//            } else {
//                ""
//            }
//        }
//    }
}
