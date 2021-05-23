package com.metriql.task

import com.metriql.util.SuccessMessage
import org.rakam.server.http.HttpService
import org.rakam.server.http.annotations.Api
import org.rakam.server.http.annotations.ApiOperation
import java.util.UUID
import javax.inject.Inject
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.QueryParam

@Path("/api/task")
@Api(value = "/api/task", nickname = "task", description = "Task Service Endpoints", tags = ["task"])
class TaskHttpService @Inject constructor(val service: TaskQueueService) : HttpService() {

    @ApiOperation(value = "View task status")
    @Path("/status")
    @GET
    fun status(@QueryParam("taskId") taskId: UUID): Task.TaskTicket<*> {
        return service.status(taskId)
    }

    @ApiOperation(value = "Cancel a query")
    @Path("/cancel")
    @POST
    fun cancelQuery(taskId: UUID): SuccessMessage {
        service.cancel(taskId)
        return SuccessMessage.success()
    }
}
