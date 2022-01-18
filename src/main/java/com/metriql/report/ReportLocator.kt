package com.metriql.report

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.ServiceLoader

object ReportLocator {
    private var services: List<ReportType> = ServiceLoader.load(ReportType::class.java).toList()

    @JvmStatic
    fun getReportType(slug: String): ReportType {
        val service = services.find { it.slug == slug }
        return service ?: throw MetriqlException("Unknown report type: $slug", HttpResponseStatus.BAD_REQUEST)
    }

    fun getList() = services

    @JvmStatic
    fun reload() {
        services = ServiceLoader.load(ReportType::class.java).toList()
    }
}

class RecipeQueryJsonDeserializer : JsonDeserializer<RecipeQuery>() {
    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext?): RecipeQuery {
        val tree = jp.readValueAsTree<ObjectNode>()
        val type = tree.get("type").textValue()
        val dataSourceClass = ReportLocator.getReportType(type).recipeClass
        return try {
            return null!!
//            JsonHelper.convert(tree, dataSourceClass)
        } catch (e: Exception) {
            throw MetriqlException("Invalid config: ${e.message}", HttpResponseStatus.BAD_REQUEST)
        }
    }
}

class ServiceReportOptionJsonDeserializer : JsonDeserializer<ServiceReportOptions>() {
    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext?): ServiceReportOptions {
        val tree = jp.readValueAsTree<ObjectNode>()
        val type = tree.get("type").textValue()
        val dataSourceClass = ReportLocator.getReportType(type).configClass
        return try {
            return null!!
//            JsonHelper.convert(tree, dataSourceClass)
        } catch (e: Exception) {
            throw MetriqlException("Invalid config: ${e.message}", HttpResponseStatus.BAD_REQUEST)
        }
    }
}
