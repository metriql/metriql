package com.metriql.service.jdbc

import com.google.common.base.Splitter
import com.google.common.net.HttpHeaders.X_FORWARDED_PROTO
import com.metriql.db.QueryResult
import com.metriql.deployment.Deployment
import com.metriql.report.ReportLocator
import com.metriql.report.ReportService
import com.metriql.report.mql.MqlReportOptions
import com.metriql.report.mql.MqlReportType
import com.metriql.report.sql.SqlReportOptions
import com.metriql.report.sql.SqlReportType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.task.Task
import com.metriql.service.task.TaskQueueService
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.warehouse.metriql.CatalogFile
import io.airlift.jaxrs.testing.GuavaMultivaluedMap
import io.airlift.json.ObjectMapperProvider
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.HttpHeaders.Names.HOST
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.MetriqlConnectorFactory.Companion.QUERY_TYPE_PROPERTY
import io.trino.MetriqlMetadata.Companion.getTrinoType
import io.trino.Query.formatType
import io.trino.Query.toClientTypeSignature
import io.trino.client.Column
import io.trino.client.FailureInfo
import io.trino.client.QueryError
import io.trino.client.QueryResults
import io.trino.client.StageStats
import io.trino.client.StatementStats
import io.trino.server.HttpRequestSessionContext
import io.trino.spi.ErrorType
import io.trino.spi.StandardErrorCode
import io.trino.spi.security.Identity
import io.trino.sql.analyzer.TypeSignatureTranslator
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.tree.Execute
import io.trino.sql.tree.Query
import io.trino.sql.tree.Statement
import io.trino.testing.TestingGroupProvider
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import org.rakam.server.http.annotations.QueryParam
import java.net.URI
import java.time.Instant
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer
import java.util.logging.Level
import java.util.logging.Logger
import javax.inject.Named
import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.core.MultivaluedMap

@Path("/v1/statement")
class StatementService(
    private val taskQueueService: TaskQueueService,
    private val reportService: ReportService,
    private val deployment: Deployment
) : HttpService() {
    private val runner = LightweightQueryRunner(deployment.getModelService())
    private val mapper = ObjectMapperProvider().get()
    private val groupProviderManager = TestingGroupProvider()

    fun startServices(catalogs: CatalogFile.Catalogs?) {
        runner.start(catalogs)
    }

    companion object {
        val defaultParsingOptions = ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL)
        private val logger = Logger.getLogger(this::class.java.name)
    }

    private fun isMetadataQuery(statement: Statement, defaultCatalog: String): Boolean {
        val isMetadata = AtomicReference<Boolean?>()

        return if (statement !is Query) {
            true
        } else {
            IsMetriqlQueryVisitor(defaultCatalog).process(statement, isMetadata)
            isMetadata.get()?.let { !it } ?: false
        }
    }

    private fun createSessionContext(request: RakamHttpRequest): HttpRequestSessionContext {
        val headerMap: MultivaluedMap<String, String> = GuavaMultivaluedMap()
        request.headers().forEach(
            Consumer { header: Map.Entry<String, String> ->
                headerMap.add(
                    header.key,
                    header.value
                )
            }
        )
        return HttpRequestSessionContext(
            headerMap, Optional.of("Presto"), request.uri,
            Optional.of(Identity.ofUser("default")), groupProviderManager
        )
    }

    @Path("/")
    @POST
    fun query(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth) {
        request.bodyHandler { body ->
            val sql = String(body.readAllBytes())
            println("$sql")

            val sessionContext = createSessionContext(request)

            val auth = auth.copy(userId = sessionContext.identity.user, source = sessionContext.source)

            val mode = sessionContext.systemProperties?.get(QUERY_TYPE_PROPERTY.name) ?: QUERY_TYPE_PROPERTY.defaultValue
            val reportType = ReportLocator.getReportType(mode)

            val rawStmt = try {
                runner.runner.sqlParser.createStatement(sql, defaultParsingOptions)
            } catch (e: Exception) {
                // since it's a trino query, let the executor handle the exception
                null
            }

            val (statement, parameters) = if (rawStmt is Execute) {
                val ps = sessionContext.preparedStatements[rawStmt.name.value]
                runner.runner.sqlParser.createStatement(ps, defaultParsingOptions) to rawStmt.parameters
            } else rawStmt to null

            val task = if (reportType == MqlReportType && (statement == null || isMetadataQuery(statement, "metriql"))) {
                runner.createTask(auth, sessionContext, sql)
            } else {
                val dataSource = deployment.getDataSource(auth)
                val context = reportService.createContext(auth, dataSource)

                val finalSql = if (rawStmt is Execute) {
                    sessionContext.preparedStatements[rawStmt.name.value]!!
                } else sql

                val options = when (reportType) {
                    MqlReportType -> MqlReportOptions(finalSql, null, parameters, null)
                    SqlReportType -> SqlReportOptions(finalSql, null, null, null)
                    else -> JsonHelper.read(finalSql, reportType.recipeClass.java).toReportOptions(context)
                }

                reportService.queryTask(
                    auth,
                    reportType,
                    dataSource,
                    options,
                    context = context
                )
            }

            taskQueueService.execute(task, -1).thenAccept {
                try {
                    val trinoQueryResult = mapper.writeValueAsBytes(convertQueryResult(request, it.taskTicket(), true))
                    request.addResponseHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json")
                    request.response(trinoQueryResult, HttpResponseStatus.OK).end()
                } catch (e: Throwable) {
                    logger.log(Level.WARNING, "Unknown exception thrown running query", e)
                }
            }
        }
    }

    private fun convertQueryResult(request: RakamHttpRequest, task: Task.TaskTicket<QueryResult>, firstCall: Boolean = false): QueryResults {

        val id = task.id.toString()
        val columns = task?.result?.metadata?.map {
            val trinoType = getTrinoType(it.type)
            val formatted = formatType(TypeSignatureTranslator.toSqlType(trinoType))
            Column(it.name, formatted, toClientTypeSignature(trinoType.typeSignature))
        }

        val uri = request.headers().get(HOST)
        task.result?.responseHeaders?.let { headers ->
            headers.forEach { (key, value) ->
                request.addResponseHeader(key, value)
            }
        }

        val elapsedTime = Instant.now().toEpochMilli() - task.startedAt.toEpochMilli()
        val taskStatus = if (firstCall) Task.Status.QUEUED else task.status
        val stats = StatementStats.builder()
            .setState(taskStatus.name)
            .setNodes(if (taskStatus == Task.Status.QUEUED) 0 else 1)
            .setElapsedTimeMillis(elapsedTime)
            .setQueuedTimeMillis(elapsedTime)
            .setQueued(taskStatus == Task.Status.QUEUED)
            .setTotalSplits(1)

        if (task.status != Task.Status.QUEUED && !firstCall) {
            val stageStats = StageStats.builder()
                .setStageId("0")
                .setState(task.status.name)
                .setNodes(1)
                .setTotalSplits(1)
                .setSubStages(listOf())

            if (task.status.isDone) {
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

        val protoHeader = request.headers().get(X_FORWARDED_PROTO)
        val scheme = if (protoHeader != null) {
            val supportsHttps = Splitter.on(',').split(protoHeader).any { it.lowercase() == "https" }
            if (supportsHttps) "https" else "http"
        } else "http"

        val queryUri = URI("$scheme://$uri/v1/statement/queued/?id=$id")

        val error = task.result?.error?.let {
            QueryError(
                it.message + "\n" + task.result?.getProperty(QueryResult.PropertyKey.QUERY),
                it.sqlState,
                10,
                StandardErrorCode.GENERIC_USER_ERROR.name,
                ErrorType.USER_ERROR.name,
                null,
                FailureInfo("metriql", it.message, null, listOf(), listOf(), null)
            )
        }

        val results = QueryResults(
            id,
            if (task.id == null) null else queryUri,
            if (task.id == null || task.status != Task.Status.RUNNING) null else queryUri,
            if (!firstCall && (task.status.isDone || task.id == null)) null else queryUri,
            if (firstCall) null else columns,
            if (firstCall) null else (task.result?.let { transformQueryResult(it) }),
            stats.build(),
            if (firstCall) null else error,
            listOf(),
            task.result?.properties?.get(QUERY_TYPE) as String?,
            null
        )

        return results
    }

    private fun transformQueryResult(result: QueryResult): Iterable<List<Any?>>? {
        return result.result
//        if (result.metadata == null || result.result == null) {
//            return null
//        }
//
//        val types = result.metadata?.map { it.type ?: FieldType.UNKNOWN }
//
//        if (types.none { it == FieldType.TIMESTAMP } || result.getProperty(CACHE_TIME) != null) {
//            return result.result
//        }
//
//        return result.result?.map { row ->
//            row.mapIndexed { idx, cell ->
//                when (types[idx]) {
//                    FieldType.TIMESTAMP -> {
//                        val localDateTime = cell as LocalDateTime
//                        val zone = TimeZoneKey.UTC_KEY
//                        SqlTimestampWithTimeZone.newInstance(3, localDateTime.atZone(zone.zoneId).toEpochSecond() * 1000, 0, zone)
//                    }
//                    else -> cell
//                }
//            }
//        }
    }

    @Path("/queued")
    @GET
    fun status(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, @QueryParam("id") id: String) {
        val idUUID = try {
            UUID.fromString(id)
        } catch (e: Exception) {
            throw MetriqlException(HttpResponseStatus.NOT_FOUND)
        }
        val task = taskQueueService.status(idUUID)
        request.addResponseHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json")
        val response = mapper.writeValueAsBytes(convertQueryResult(request, task as Task.TaskTicket<QueryResult>))
        request.response(response).end()
    }

    @DELETE
    @Path("/queued")
    fun delete(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, @QueryParam("id") id: String, @QueryParam("maxWait", required = false) maxWait: String?) {
        taskQueueService.cancel(UUID.fromString(id))
        request.response(byteArrayOf(), HttpResponseStatus.OK)
    }
}
