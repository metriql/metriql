package com.metriql.audit

import com.fasterxml.jackson.databind.JsonNode
import com.metriql.auth.ProjectAuth
import com.metriql.db.QueryResult
import com.metriql.model.DimensionName
import com.metriql.model.ModelName
import com.metriql.report.ReportType
import com.metriql.task.Task
import com.metriql.warehouse.spi.services.ServiceReportOptions
import org.rakam.server.http.RakamHttpRequest

class MetriqlEvents {

    data class Exception(val request: RakamHttpRequest, val e: java.lang.Exception)
    data class InternalException(val e: Throwable, val userId: Int?, val projectId: Int?)
    data class UnhandledTaskException(val e: Throwable, val task: Task<*, *>)

    sealed class AuditLog(open val auth: ProjectAuth, val type: Int) {
        data class RecipeUpdateHookEvent(override val auth: ProjectAuth, val body: JsonNode, val recipeId: Int) : AuditLog(auth, 2)

        data class SQLExecuteEvent(
            override val auth: ProjectAuth,
            val query: String,
            val queryType: SQLContext?,
            val value: Any?,
            val durationInMillis: Long,
            val error: QueryResult.QueryError?
        ) : AuditLog(auth, 1) {
            enum class SQLContext {
                ADHOC_REPORT, SUGGESTION;

                data class AdhocReport(val type: ReportType, val options: ServiceReportOptions)
                data class Suggestion(val modelName: ModelName, val dimensionName: DimensionName, val filter: String?)
            }

            override fun toString(): String {
                return "Query executed by: ${auth.userId ?: "system"} on project: $auth.projectId\n$query"
            }
        }
    }
}
