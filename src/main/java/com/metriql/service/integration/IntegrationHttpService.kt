package com.metriql.service.integration

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metriql.CURRENT_PATH
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
import javax.inject.Named
import javax.ws.rs.GET
import javax.ws.rs.Path

@Path("$CURRENT_PATH/integration")
class IntegrationHttpService(val modelService: IModelService) : HttpService() {
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
        val models = modelService.list(auth)
        val stdin = JsonHelper.encodeAsBytes(models)

        val apiUrl = request.headers().get("origin") ?: request.headers().get("host")?.let { "http://$it" }
        ?: throw MetriqlException("Unable to identify metriql url", BAD_REQUEST)
        val commands = listOf("metriql-tableau", "--metriql-url", apiUrl, "--dataset", dataset, "create-tds")
        runCommand(request, commands, stdin, "$dataset.tds")
    }

    @Path("/looker")
    @GET
    fun looker(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, @QueryParam("connection") connection: String) {
        val models = modelService.list(auth)
        val stdin = JsonHelper.encodeAsBytes(models)
        runCommand(request, listOf("metriql-lookml", "--connection", connection), stdin, "$connection.zip")
    }

    private fun runCommand(request: RakamHttpRequest, commands: List<String>, stdin: ByteArray, fileName: String?) {
        executor.execute {
            try {
                val process = ProcessBuilder().command(commands).start()

                val outputStream = process.outputStream
                outputStream.write(stdin)
                outputStream.write(System.lineSeparator().toByteArray())
                outputStream.flush()

                val exitVal = try {
                    process.waitFor(20, TimeUnit.SECONDS)
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
                returnError(request, "Unknown error executing command: ${e}", INTERNAL_SERVER_ERROR)
            }
        }
    }
}
