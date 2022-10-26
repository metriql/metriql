package com.metriql.warehouse.spi.services

import com.fasterxml.jackson.annotation.JsonIgnore
import com.hubspot.jinjava.Jinjava
import com.metriql.db.JSONBSerializable
import com.metriql.report.ReportType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.DatasetName
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

@JSONBSerializable
interface ServiceQuery {
    @JsonIgnore
    fun getQueryLimit(): Int? = null
}

@JSONBSerializable
interface RecipeQuery {
    @JsonIgnore
    fun toReportOptions(context: IQueryGeneratorContext): ServiceQuery
}

@JSONBSerializable
interface MaterializeQuery {
    fun getModelName(): String? = null
    fun toQuery(datasetName: DatasetName): ServiceQuery

    companion object {
        fun defaultModelName(modelName: String, reportType: ReportType, name: String): String {
            return "${modelName}_${reportType.slug}_$name"
        }
    }
}

interface ServiceSupport

interface ServiceQueryDSL

interface ServiceQueryGenerator<T : ServiceQueryDSL, K : ServiceQuery, C : ServiceSupport> {
    fun generateSQL(auth: ProjectAuth, context: IQueryGeneratorContext, queryDSL: T, options: K): String
    fun supports(): List<C> = listOf()

    val jinja: Jinjava get() = defaultJinja
    companion object {
        private val defaultJinja = Jinjava()
    }
}
