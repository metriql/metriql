package com.metriql.service.jdbc

import io.airlift.units.Duration
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.client.NodeVersion
import io.trino.client.ServerInfo
import io.trino.metadata.NodeState
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import java.time.Instant
import java.util.Optional
import java.util.concurrent.TimeUnit
import javax.ws.rs.GET
import javax.ws.rs.Path

@Path("/v1/info")
class NodeInfoService : HttpService() {
    val startTime = Instant.now()!!

    @GET
    @Path("/")
    fun main(): ServerInfo {
        val durationInSeconds = Instant.now().epochSecond - startTime.epochSecond
        return ServerInfo(
            NodeVersion.UNKNOWN,
            "dev",
            true,
            false,
            Optional.of(Duration(durationInSeconds.toDouble(), TimeUnit.SECONDS))
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
