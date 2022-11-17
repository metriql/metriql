package com.metriql.report

import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.metriql.warehouse.spi.services.ServiceQuery
import kotlin.reflect.KClass

@JsonDeserialize(using = ReportType.ReportTypeJsonDeserializer::class)
interface ReportType {
    val slug: String
    val dataClass: KClass<out ServiceQuery>
    val serviceClass: KClass<out IAdHocService<out ServiceQuery>>

    @JsonValue
    fun getJsonValue() = slug

    class ReportTypeJsonDeserializer : JsonDeserializer<ReportType>() {
        override fun deserialize(jp: JsonParser, ctxt: DeserializationContext?): ReportType {
            return ReportLocator.getReportType(jp.valueAsString)
        }
    }
}

abstract class ReportTypeProxy(val reportType: ReportType) {
    override fun equals(other: Any?) = reportType == other
    override fun hashCode() = reportType.hashCode()
}
