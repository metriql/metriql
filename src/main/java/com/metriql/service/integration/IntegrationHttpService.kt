package com.metriql.service.integration

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metriql.CURRENT_PATH
import com.metriql.Commands
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR
import org.rakam.server.http.HttpServer.returnError
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import org.rakam.server.http.annotations.QueryParam
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import javax.inject.Named
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.javaGetter

@Path("$CURRENT_PATH/integration")
class IntegrationHttpService(val deployment: Commands.Deployment) : HttpService() {
    // Run all the commands in a separate thread in order to avoid race conditions
    val executor = ThreadPoolExecutor(
        0, 1, 30L, TimeUnit.SECONDS,
        LinkedBlockingQueue(),
        ThreadFactoryBuilder()
            .setNameFormat("integration-executor")
            .setDaemon(true)
            .build()
    )

    @Path("/tableau")
    @GET
    fun tableau(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, @QueryParam("dataset") dataset: String) {
        val models = deployment.getModelService().list(auth)
        val stdin = JsonHelper.encodeAsBytes(models)

        val apiUrl = request.headers().get("origin") ?: request.headers().get("host")?.let { "http://$it" }
            ?: throw MetriqlException("Unable to identify metriql url", BAD_REQUEST)
        val commands = listOf("metriql-tableau", "--metriql-url", apiUrl, "--dataset", dataset, "create-tds")
        runCommand(request, commands, stdin, "$dataset.tds")
    }

    @Path("/looker")
    @GET
    fun looker(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, @QueryParam("connection") connection: String) {
        val models = deployment.getModelService().list(auth)
        val stdin = JsonHelper.encodeAsBytes(models)
        runCommand(request, listOf("metriql-lookml", "--connection", connection), stdin, "$connection.zip")
    }

    @Path("/superset")
    @POST
    fun superset(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, action: SupersetAction, parameters: SupersetParameters) {
        runCommand(
            request, listOf("metriql-superset") + listOf(action.name) + buildArguments(parameters),
            if (action.needsStdin) {
                val models = deployment.getModelService().list(auth)
                JsonHelper.encodeAsBytes(models)
            } else null
        )
    }

    @Path("/metabase")
    @POST
    fun metabase(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, action: MetabaseAction, parameters: MetabaseParameters) {
        runCommand(
            request, listOf("metriql-metabase") + listOf(action.name) + buildArguments(parameters),
            if (action.needsStdin) {
                val models = deployment.getModelService().list(auth)
                JsonHelper.encodeAsBytes(models)
            } else null
        )
    }

    enum class SupersetAction(val needsStdin: Boolean) {
        `list-databases`(false), `sync-database`(true)
    }

    enum class MetabaseAction(val needsStdin: Boolean) {
        `list-databases`(false), `sync-database`(true)
    }

    data class SupersetParameters(val database_id: Int?, val database_name: String?, val superset_url: String, val superset_username: String, val superset_password: String)
    data class MetabaseParameters(val database_name: String?, val metabase_url: String, val metabase_username: String, val metabase_password: String)

    private fun buildArguments(dataObject: Any): List<String> {
        return dataObject::class.declaredMemberProperties
            .flatMap { prop -> prop.javaGetter?.invoke(dataObject)?.let { listOf("--${prop.name.replace('_', '-')}", it.toString()) } ?: listOf() }
    }

    private fun runCommand(request: RakamHttpRequest, commands: List<String>, stdin: ByteArray? = null, fileName: String? = null) {
        executor.execute {
            try {
                logger.info("Executing command `${commands.joinToString(" ")}`")
                val process = ProcessBuilder().command(commands).start()

                if (stdin != null) {
                    val outputStream = process.outputStream
                    outputStream.write(stdin)
                    outputStream.write(System.lineSeparator().toByteArray())
                    outputStream.flush()
                }

                val exitVal = try {
                    process.waitFor(240, TimeUnit.SECONDS)
                } catch (e: Exception) {
                    returnError(request, "Unable to execute: ${e.message}", INTERNAL_SERVER_ERROR)
                    return@execute
                }

                if (exitVal) {
                    val result = process.inputStream.bufferedReader().readText()
                    val error = process.errorStream.bufferedReader().readText()

                    if (error.isEmpty()) {
                        if (fileName != null) {
                            request.addResponseHeader(HttpHeaders.Names.CONTENT_TYPE, "application/octet-stream")
                            request.addResponseHeader("Content-Disposition", "attachment;filename=$fileName")
                            request.addResponseHeader(HttpHeaders.Names.ACCESS_CONTROL_EXPOSE_HEADERS, "Content-Disposition,X-Suggested-Filename")
                        }
                        request.response(result).end()
                    } else {
                        returnError(request, error, BAD_REQUEST)
                    }
                } else {
                    try {
                        process.destroyForcibly()
                    } catch (e: Exception) {
                    }

                    returnError(request, "Unable to run command: timeout after 20 seconds", INTERNAL_SERVER_ERROR)
                }
            } catch (e: Exception) {
                returnError(request, "Unknown error executing command: $e", INTERNAL_SERVER_ERROR)
            }
        }
    }

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)
    }
}
