package com.metriql.service.integration

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metriql.CURRENT_PATH
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.util.JsonHelper
import io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import org.rakam.server.http.annotations.JsonRequest
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import javax.inject.Named
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
    @JsonRequest
    fun tableau(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth, dataset: String) {
        val model = modelService.getModel(auth, dataset)
        val stdin = JsonHelper.encodeAsBytes(model)

        runCommand(request, arrayOf("metriql2tableau", "--metriql-url", "http://123", "--dataset", dataset), stdin)
    }

    @Path("/looker")
    @JsonRequest
    fun looker(request: RakamHttpRequest, @Named("userContext") auth: ProjectAuth) {
        val models = modelService.list(auth)
        val stdin = JsonHelper.encodeAsBytes(models)

        runCommand(request, arrayOf("metriql2lookml", "--metriql-url", "http://123"), stdin)
    }

    private fun runCommand(request: RakamHttpRequest, commands: Array<String>, stdin: ByteArray) {
        executor.execute {
            try {
                var process = Runtime.getRuntime().exec(
                    commands,
                    arrayOf()
                )
                process.outputStream.write(stdin)

                val exitVal = try {
                    process.waitFor(20, TimeUnit.SECONDS)
                } catch (e: Exception) {
                    request.response("Unable to execute: ${e.message}", INTERNAL_SERVER_ERROR).end()
                    return@execute
                }

                if (exitVal) {
                    val result = process.inputStream.bufferedReader().readText()
                    process.errorStream.bufferedReader().readText()
                    request.response(result).end()
                } else {
                    try {
                        process.destroyForcibly()
                    } catch (e: Exception) {
                    }

                    request.response("Unable to run command: timeout", INTERNAL_SERVER_ERROR).end()
                }
            } catch (e: Exception) {
                request.response("Unknown error executing command: ${e.message}", INTERNAL_SERVER_ERROR).end()
            }
        }
    }
}
