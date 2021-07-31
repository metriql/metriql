package com.metriql

import com.metriql.util.HttpUtil.sanitizeUri
import com.metriql.util.HttpUtil.sendError
import com.metriql.util.HttpUtil.sendFile
import com.metriql.util.TextUtil
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import java.io.File
import javax.ws.rs.GET
import javax.ws.rs.Path

const val CURRENT_VERSION = "v0"
const val CURRENT_PATH = "/api/$CURRENT_VERSION"

@Path("/")
class BaseHttpService : HttpService() {
    val directory: File = File("/app/frontend").absoluteFile

    @Path("/")
    @GET
    fun main(request: RakamHttpRequest) {
        val response = DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SEE_OTHER)
        response.headers().set(HttpHeaders.Names.LOCATION, "/ui")
        request.response(response).end()
    }

    @Path("/ui")
    @GET
    fun uiMain(request: RakamHttpRequest) {
        ui(request)
    }

    @Path("/ui/*")
    @GET
    fun ui(request: RakamHttpRequest) {
        if (!request.decoderResult.isSuccess) {
            sendError(request, HttpResponseStatus.BAD_REQUEST)
            return
        }

        if (request.method !== HttpMethod.GET) {
            sendError(request, HttpResponseStatus.NOT_FOUND)
            return
        }

        val uri: String = request.path()
        val requestedFile = File(sanitizeUri(directory, uri, prefix = "/ui")).absoluteFile
        if (!requestedFile.startsWith(directory.absolutePath)) {
            sendError(request, HttpResponseStatus.NOT_FOUND)
        } else {
            val file = if (requestedFile.isDirectory || !requestedFile.exists()) File(directory, "index.html") else requestedFile
            sendFile(request, file)
        }
    }

    @Path("/version")
    @GET
    fun version(): String? {
        return TextUtil.version()
    }
}
