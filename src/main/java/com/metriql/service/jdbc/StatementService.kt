package com.metriql.service.jdbc

import com.metriql.db.QueryResult
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.report.sql.SqlReportOptions
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.service.task.Task
import com.metriql.service.task.TaskQueueService
import com.metriql.warehouse.spi.DataSource
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.MetriqlMetadata.Companion.getTrinoType
import io.trino.client.ClientTypeSignature
import io.trino.client.Column
import io.trino.client.QueryError
import io.trino.client.QueryResults
import io.trino.client.StatementStats
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import java.net.URI
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.QueryParam

@Path("/v1/statement")
class StatementService(val taskQueueService: TaskQueueService, val reportService: ReportService, val dataSource: DataSource, val modelService: IModelService) : HttpService() {
    private val runner = LightweightQueryRunner(modelService.list(ProjectAuth.singleProject(null)))

    companion object {
        val parser = SqlParser()
        val nativeRegex = "[ ]*\\-\\-[ ]*\\@mode\\:([a-zA-Z]+) ".toRegex()
    }

    private fun isMetadataQuery(sql: String): Boolean {
        val isMetadata = AtomicReference<Boolean?>()
        IsMetadataQueryVisitor().process(parser.createStatement(sql, ParsingOptions()), isMetadata)
        return isMetadata.get() ?: false
    }

    @Path("/")
    @POST
    fun query(request: RakamHttpRequest): CompletableFuture<QueryResults> {
        val future = CompletableFuture<QueryResults>()

        request.bodyHandler {
            val sql = String(it.readAllBytes())

            val auth = ProjectAuth.singleProject(null)

            val mode = nativeRegex.find(sql)?.groupValues?.get(0)

            val reportType = if (mode == "sql") {
                ReportType.SQL
            } else {
                ReportType.MQL
            }

            println("$reportType $sql")
            val task = if (reportType == ReportType.MQL && isMetadataQuery(sql)) {
                runner.createTask(auth, sql)
            } else {
                reportService.queryTask(
                    auth,
                    reportType,
                    dataSource,
                    SqlReportOptions(sql, null, null, null)
                )
            }

            taskQueueService.execute(task, 0)

            future.complete(convertQueryResult(request.uri, task.taskTicket()))
        }

        return future
    }

    private fun convertQueryResult(uri: String, task: Task.TaskTicket<QueryResult>): QueryResults {
        val id = task.id.toString()
        val columns = task?.result?.metadata?.map {
            val trinoType = getTrinoType(it.type)
            Column(it.name, trinoType.baseName, ClientTypeSignature(trinoType.baseName))
        }
        if(task.isDone()) {
            println(task.result?.result?.size)
        }
        return QueryResults(
            id,
            URI("http://app.rakam.io/query"),
            null,
            if (task.isDone()) null else URI("http://127.0.0.1:5656/v1/statement/queued/?id=$id"),
            columns,
            task.result?.result,
            StatementStats.builder().setState(task.status.name).build(),
            task.result?.error?.let { QueryError(it.message, it.sqlState, 10, null, null, null, null) },
            listOf(),
            null,
            null
        )
    }

    @Path("/queued")
    @GET
    fun status(request: RakamHttpRequest, @QueryParam("id") id: String, @QueryParam("maxWait") maxWait: Duration?): CompletableFuture<QueryResults> {
        val task = taskQueueService.status(UUID.fromString(id))
        return CompletableFuture.completedFuture(convertQueryResult(request.uri, task as Task.TaskTicket<QueryResult>))
    }

    @DELETE
    @Path("/queued")
    fun delete(request: RakamHttpRequest, @QueryParam("id") id: String, @QueryParam("maxWait") maxWait: Duration?) {
        taskQueueService.cancel(UUID.fromString(id))
        request.response(byteArrayOf(), HttpResponseStatus.OK)
    }
}
