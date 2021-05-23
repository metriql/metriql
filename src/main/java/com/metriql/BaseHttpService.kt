package com.metriql

import org.rakam.server.http.HttpService
import javax.ws.rs.GET
import javax.ws.rs.Path

const val CURRENT_VERSION = "/api/v0"

@Path("/")
class BaseHttpService : HttpService() {
    @Path("/")
    @GET
    fun main(): String {
        return "Please use $CURRENT_VERSION path"
    }

    @Path("/version")
    @GET
    fun version(): String? {
        return this.javaClass.getPackage().implementationVersion
    }
}
