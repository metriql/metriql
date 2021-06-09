package com.metriql.service.jdbc

import com.metriql.service.task.TaskQueueService
import io.trino.execution.QueryInfo
import io.trino.server.BasicQueryInfo
import org.rakam.server.http.HttpService
import org.rakam.server.http.RakamHttpRequest
import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.Path

@Path("/v1/query")
class QueryService(val taskQueueService: TaskQueueService) : HttpService() {
    @GET
    @Path("/")
    fun listActiveQueries(): List<BasicQueryInfo> {
        return listOf()
    }

    @GET
    @Path("/*")
    fun activeQuery(): QueryInfo {
        return null!!
    }

    @DELETE
    @Path("/*")
    fun cancelQuery(request: RakamHttpRequest) {
        request.response(byteArrayOf())
    }
}
