package com.metriql.service.audit

import com.fasterxml.jackson.databind.JsonNode
import com.metriql.db.QueryResult
import com.metriql.report.ReportType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.DimensionName
import com.metriql.service.model.ModelName
import com.metriql.service.task.Task
import com.metriql.warehouse.spi.services.ServiceReportOptions
import org.rakam.server.http.RakamHttpRequest

class MetriqlEvents {

    data class Exception(val request: RakamHttpRequest, val e: java.lang.Exception)
    data class InternalException(val e: Throwable, val userId: Any?, val projectId: Int?)
    data class UnhandledTaskException(val e: Throwable, val task: Task<*, *>)

    sealed class AuditLog(open val auth: ProjectAuth, val type: Int) {
        data class RecipeUpdateHookEvent(override val auth: ProjectAuth, val body: JsonNode, val recipeId: Int) : AuditLog(auth, 2)

        data class SQLExecuteEvent(
            override val auth: ProjectAuth,
            val query: String,
            val value: SQLContext?,
            val durationInMillis: Long,
            val error: QueryResult.QueryError?
        ) : AuditLog(auth, 1) {
            sealed class SQLContext(val type: String) {
                data class AdhocReport(val reportType: ReportType, val options: ServiceReportOptions) : SQLContext("ADHOC_REPORT")
                data class Suggestion(val modelName: ModelName, val dimensionName: DimensionName, val filter: String?) : SQLContext("SUGGESTION")
            }

            override fun toString(): String {
                return "Query executed by: ${auth.userId ?: "system"} on project: $auth.projectId\n$query"
            }
        }
    }
}
