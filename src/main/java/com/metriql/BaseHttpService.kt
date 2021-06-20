package com.metriql

import com.metriql.util.TextUtil
import org.rakam.server.http.HttpService
import javax.ws.rs.GET
import javax.ws.rs.Path

const val CURRENT_VERSION = "v0"
const val CURRENT_PATH = "/api/$CURRENT_VERSION"

@Path("/")
class BaseHttpService : HttpService() {
    @Path("/")
    @GET
    fun main(): String {
        return "Please use $CURRENT_PATH path"
    }

    @Path("/version")
    @GET
    fun version(): String? {
        return TextUtil.version()
    }
}
