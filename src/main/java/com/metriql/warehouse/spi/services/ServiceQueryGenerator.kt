package com.metriql.warehouse.spi.services

import com.fasterxml.jackson.annotation.JsonIgnore
import com.hubspot.jinjava.Jinjava
import com.metriql.auth.ProjectAuth
import com.metriql.db.JSONBSerializable
import com.metriql.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus

@JSONBSerializable
interface ServiceReportOptions {
    @JsonIgnore
    fun toRecipeQuery(): RecipeQuery

    @JsonIgnore
    fun getQueryLimit(): Int? = null
}

@JSONBSerializable
interface RecipeQuery {
    @JsonIgnore
    fun toReportOptions(context: IQueryGeneratorContext): ServiceReportOptions

    @JsonIgnore
    fun toMaterialize(): MaterializeQuery {
        throw MetriqlException("Report type doesn't support materialization", HttpResponseStatus.NOT_IMPLEMENTED)
    }
}

@JSONBSerializable
interface MaterializeQuery {
    fun toQuery(modelName: ModelName): RecipeQuery
}

interface ServiceSupport

interface ServiceQueryDSL

interface ServiceQueryGenerator<T : ServiceQueryDSL, K : ServiceReportOptions, C : ServiceSupport> {
    fun generateSQL(auth: ProjectAuth, context: IQueryGeneratorContext, queryDSL: T, options: K): String
    fun supports(): List<C> = listOf()

    val jinja: Jinjava get() = defaultJinja
    companion object {
        private val defaultJinja = Jinjava()
    }
}

@UppercaseEnum
enum class ServiceType {
    SEGMENTATION, FUNNEL, RETENTION, FLOW
}
