package com.metriql.service.task

import com.metriql.CURRENT_PATH
import com.metriql.util.SuccessMessage
import org.rakam.server.http.HttpService
import org.rakam.server.http.annotations.Api
import org.rakam.server.http.annotations.ApiOperation
import org.rakam.server.http.annotations.QueryParam
import java.util.UUID
import javax.inject.Inject
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path

@Path("$CURRENT_PATH/task")
@Api(value = "$CURRENT_PATH/task", nickname = "task", description = "Task Service Endpoints", tags = ["task"])
class TaskHttpService @Inject constructor(val service: TaskQueueService) : HttpService() {

    @ApiOperation(value = "View task status")
    @Path("/status")
    @GET
    fun status(@QueryParam("taskId") taskId: String): Task.TaskTicket<*> {
        return service.status(UUID.fromString(taskId))
    }

    @ApiOperation(value = "List all tasks")
    @Path("/list")
    @GET
    fun list(): List<Task.TaskTicket<out Any?>> {
        return service.currentTasks()
    }

    @ApiOperation(value = "Cancel a query")
    @Path("/cancel")
    @POST
    fun cancelQuery(taskId: UUID): SuccessMessage {
        service.cancel(taskId)
        return SuccessMessage.success()
    }
}
