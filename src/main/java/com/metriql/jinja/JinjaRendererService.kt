package com.metriql.jinja

import com.hubspot.jinjava.Jinjava
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.hubspot.jinjava.lib.fn.ELFunctionDefinition
import com.hubspot.jinjava.lib.tag.Tag
import com.hubspot.jinjava.tree.TagNode
import com.hubspot.jinjava.util.LengthLimitingStringBuilder
import com.metriql.auth.ProjectAuth
import com.metriql.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.util.ValidationUtil.quoteIdentifier
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.filter.DateRange
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import javax.inject.Inject

/**
 * SqlRenderable
 * */
class JinjaRendererService @Inject constructor() {
    val jinja: Jinjava = Jinjava()

    init {
        val methods = Functions::class.java.declaredMethods
        methods.forEach {
            jinja.globalContext.registerFunction(ELFunctionDefinition("", it.name, it))
        }

        jinja.globalContext.registerTag(RealtimeTag())
    }

    fun render(
        auth: ProjectAuth,
        dataSource: DataSource,
        sqlRenderable: SQLRenderable,
        modelName: ModelName?,
        context: IQueryGeneratorContext,
        inQueryDimensionNames: List<String>? = null,
        dateRange: DateRange? = null,
        targetModelName: ModelName? = null,
        variables: Map<String, Any> = mapOf(),
        hook: ((Map<String, Any?>) -> Map<String, Any?>)? = null,
    ): String {
        /*
        * select {{model.firebase_event_in_app_purchase.dimension.user__region_code}}, count(*)
        * from {{model.firebase_event_in_app_purchase}}
        * where {{model.firebase_event_in_app_purchase.dimension.event_timestamp}} between {{date.start}} and {{date.end}}
        * group by 1
        *
        * The query above renders user__region_code dimension on firebase_event_in_app_purchase model. However, that dimension is in SQL context with in_query checking
        * We have to include the columns and dimension names with the same modelname to in_query context so that `firebase_event_in_app_purchase` SQL model
        * will get rendered including that dimension
        * */
        val additionalDimensionsAndColumns = if (modelName != null) {
            val columns = context.columns.filter { it.first == modelName }.map { it.second }
            val dimensions = context.dimensions.filter { it.key.first == modelName }.map { it.key.second }
            columns + dimensions
        } else {
            listOf()
        }

        val base = mapOf(
            "TABLE" to if (modelName != null) quoteIdentifier(modelName, dataSource.warehouse.bridge.aliasQuote) else null,
            "TARGET" to if (targetModelName != null) quoteIdentifier(targetModelName, dataSource.warehouse.bridge.aliasQuote) else null,
            "aq" to context.getAliasQuote(),
            "model" to MetriqlJinjaContext.ModelContext(context, auth.timezone),
            "relation" to MetriqlJinjaContext.RelationContext(modelName, context, auth.timezone),
            "dimension" to MetriqlJinjaContext.DimensionContext(modelName, context, auth.timezone),
            "measure" to MetriqlJinjaContext.MeasureContext(modelName, context, auth.timezone),
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
            throw MetriqlException("Error while rendering jinja expression `$sqlRenderable` for model `$modelName`: ${e.message}", HttpResponseStatus.BAD_REQUEST)
        }
    }

    class RealtimeTag : Tag {
        var active = false

        override fun getName(): String {
            return "realtime"
        }

        override fun interpret(node: TagNode, interpreter: JinjavaInterpreter): String {
            return if (interpreter.context["in_query"] != null) {
                val sb = LengthLimitingStringBuilder(interpreter.config.maxOutputSize)
                node.children.forEach { sb.append(it.render(interpreter)) }
                sb.toString()
            } else {
                ""
            }
        }
    }
}
