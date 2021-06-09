package com.metriql.service.jdbc

import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.jdbc.`$internal`.client.NodeVersion
import io.trino.jdbc.`$internal`.client.ServerInfo
import io.trino.metadata.NodeState
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import javax.ws.rs.GET
import javax.ws.rs.Path

@Path("/v1/info")
class NodeInfoService : HttpService() {

    @GET
    @Path("/")
    fun main(): ServerInfo {
        return ServerInfo(
            NodeVersion.UNKNOWN,
            null, true,
            false,
            null
        )
    }

    @GET
    @Path("/state")
    fun state(): NodeState {
        return NodeState.ACTIVE
    }

    @GET
    @Path("/coordinator")
    fun state(request: RakamHttpRequest) {
        request.response(byteArrayOf(), HttpResponseStatus.OK)
    }
}
