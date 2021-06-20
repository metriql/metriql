package com.metriql.service.jdbc

import com.metriql.db.QueryResult
import com.metriql.db.QueryResult.Companion.QUERY
import com.metriql.report.ReportService
import com.metriql.report.ReportType
import com.metriql.report.sql.SqlReportOptions
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.service.task.Task
import com.metriql.service.task.TaskQueueService
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.DataSource
import io.airlift.json.ObjectMapperProvider
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.HttpHeaders.Names.HOST
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.MetriqlMetadata.Companion.getTrinoType
import io.trino.Query.formatType
import io.trino.Query.toClientTypeSignature
import io.trino.client.Column
import io.trino.client.QueryError
import io.trino.client.QueryResults
import io.trino.client.StageStats
import io.trino.client.StatementStats
import io.trino.sql.analyzer.TypeSignatureTranslator
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import io.trino.sql.tree.Query
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import org.rakam.server.http.annotations.QueryParam
import java.net.URI
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path

@Path("/v1/statement")
class StatementService(
    val taskQueueService: TaskQueueService,
    val reportService: ReportService,
    val dataSourceFetcher: (RakamHttpRequest) -> DataSource,
    val modelService: IModelService
) : HttpService() {
    private val runner = LightweightQueryRunner(modelService.list(ProjectAuth.singleProject(null)))
    private val mapper = ObjectMapperProvider().get()

    init {
        runner.start()
    }

    companion object {
        val parser = SqlParser()
        val nativeRegex = "[ ]*\\-\\-[ ]*\\@mode\\:([a-zA-Z]+) ".toRegex()
    }

    private fun isMetadataQuery(sql: String, defaultCatalog: String): Boolean {
        val isMetadata = AtomicReference<Boolean?>()
        val stmt = try {
            parser.createStatement(sql, ParsingOptions())
        } catch (e: Exception) {
            // since it's a trino query, let the executor handle the exception
            return true
        }
        return if (stmt !is Query) {
            true
        } else {
            IsMetriqlQueryVisitor(defaultCatalog).process(stmt, isMetadata)
            isMetadata.get()?.let { !it } ?: true
        }
    }

//    @JsonInclude(JsonInclude.Include.NON_NULL)
//    class MetriqlQueryResults(
//        id: String,
//        infoUri: URI?,
//        partialCancelUri: URI?,
//        nextUri: URI?,
//        columns: List<Column>?,
//        data: List<List<Any?>>?,
//        stats: StatementStats?,
//        error: QueryError?,
//        warnings: List<Warning>?,
//        updateType: String?,
//        updateCount: Long?
//    ) :
//        QueryResults(id, infoUri, partialCancelUri, nextUri, columns, data, stats, error, warnings, updateType, updateCount)

    @Path("/")
    @POST
    fun query(request: RakamHttpRequest) {
        request.bodyHandler { body ->
            val sql = String(body.readAllBytes())
            println("$sql")

            val auth = ProjectAuth.singleProject(null)

            val mode = nativeRegex.find(sql)?.groupValues?.get(0)

            val reportType = if (mode == "sql") {
                ReportType.SQL
            } else {
                ReportType.MQL
            }

            val task = if (reportType == ReportType.MQL && isMetadataQuery(sql, "metriql")) {
                runner.createTask(auth, request, sql)
            } else {
                reportService.queryTask(
                    auth,
                    reportType,
                    dataSourceFetcher.invoke(request),
                    SqlReportOptions(sql, null, null, null)
                )
            }

            taskQueueService.execute(task, 3).thenAccept {
                val queryResult = try {
                    mapper.writeValueAsBytes(convertQueryResult(request, it.taskTicket()))
                } catch (e: Exception) {
                    byteArrayOf()
                }
                request.addResponseHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json")
                request.response(queryResult, HttpResponseStatus.OK).end()
            }
        }
    }

    private fun convertQueryResult(request: RakamHttpRequest, task: Task.TaskTicket<QueryResult>): QueryResults {

        val id = task.id.toString()
        val columns = task?.result?.metadata?.map {
            val trinoType = getTrinoType(it.type)
            val formatted = formatType(TypeSignatureTranslator.toSqlType(trinoType))
            Column(it.name.lowercase(), formatted, toClientTypeSignature(trinoType.typeSignature))
        }

        val uri = request.headers().get(HOST)
        task.result?.responseHeaders?.let { headers ->
            headers.forEach { (key, value) ->
                request.addResponseHeader(key, value)
            }
        }

        val stats = StatementStats.builder()
            .setState(task.status.name)
            .setNodes(1)
            .setElapsedTimeMillis(Instant.now().toEpochMilli() - task.startedAt.toEpochMilli())
            .setQueued(task.status == Task.Status.QUEUED)
            .setTotalSplits(1)

        if (task.status != Task.Status.QUEUED) {
            val stageStats = StageStats.builder()
                .setStageId("0")
                .setState(task.status.name)
                .setNodes(1)
                .setTotalSplits(1)
                .setSubStages(listOf())

            if (task.isDone()) {
                stageStats
                    .setCompletedSplits(1)
                    .setDone(true)
            } else {
                stageStats
                    .setCompletedSplits(0)
                    .setDone(false)
            }

            stats.setRootStage(stageStats.build())
        }

        val results = QueryResults(
            id,
            URI("http://$uri/query"),
            null,
            if (task.isDone()) null else URI("http://$uri/v1/statement/queued/?id=$id"),
            columns,
            task.result?.result,
            stats.build(),
            task.result?.error?.let { QueryError(it.message + "\n" + task.result?.properties?.get(QUERY), it.sqlState, 10, null, null, null, null) },
            listOf(),
            task.result?.properties?.get(QUERY_TYPE) as String?,
            null
        )

        if (task.isDone()) {
            println(task.result?.result?.size)
        }

        return results
    }

    @Path("/queued")
    @GET
    fun status(request: RakamHttpRequest, @QueryParam("id") id: String): QueryResults {
        val idUUID = try {
            UUID.fromString(id)
        } catch (e: Exception) {
            throw MetriqlException(HttpResponseStatus.NOT_FOUND)
        }
        val task = taskQueueService.status(idUUID)
        return convertQueryResult(request, task as Task.TaskTicket<QueryResult>)
    }

    @DELETE
    @Path("/queued")
    fun delete(request: RakamHttpRequest, @QueryParam("id") id: String, @QueryParam("maxWait", required = false) maxWait: String?) {
        taskQueueService.cancel(UUID.fromString(id))
        request.response(byteArrayOf(), HttpResponseStatus.OK)
    }
}
