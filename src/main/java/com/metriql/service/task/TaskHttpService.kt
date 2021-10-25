package com.metriql.service.task

import com.metriql.CURRENT_PATH
import com.metriql.service.auth.ProjectAuth
import com.metriql.util.MetriqlException
import com.metriql.util.SuccessMessage
import io.netty.handler.codec.http.HttpResponseStatus
import org.rakam.server.http.HttpService
import org.rakam.server.http.annotations.Api
import org.rakam.server.http.annotations.ApiOperation
import org.rakam.server.http.annotations.QueryParam
import java.util.UUID
import javax.inject.Inject
import javax.inject.Named
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path

@Path("$CURRENT_PATH/task")
@Api(value = "$CURRENT_PATH/task", nickname = "task", description = "Task Service Endpoints", tags = ["task"])
class TaskHttpService @Inject constructor(val service: TaskQueueService) : HttpService() {

    @ApiOperation(value = "View task status")
    @Path("/status")
    @GET
    fun status(@Named("userContext") auth: ProjectAuth, @QueryParam("taskId") taskId: String): Task.TaskTicket<*> {
        if (!auth.isSuperuser) {
            throw MetriqlException(HttpResponseStatus.FORBIDDEN)
        }
        return service.status(UUID.fromString(taskId))
    }

    @ApiOperation(value = "View number of active queries")
    @Path("/activeCount")
    @GET
    fun activeCount(@Named("userContext") auth: ProjectAuth): Int {
        if (!auth.isSuperuser) {
            throw MetriqlException(HttpResponseStatus.FORBIDDEN)
        }
        return service.currentTasks().count { !it.status.isDone }
    }

    @ApiOperation(value = "List all tasks")
    @Path("/list")
    @GET
    fun list(
        @Named("userContext") auth: ProjectAuth,
        @QueryParam("showResults", required = false) showResults: Boolean?,
        @QueryParam("status", required = false) status: Task.Status?,
        @QueryParam("project", required = false) projectId: Int?,
    ): List<Task.TaskTicket<out Any?>> {
        if (!auth.isSuperuser) {
            throw MetriqlException(HttpResponseStatus.FORBIDDEN)
        }
        return service.currentTasks(showResults ?: false, status, projectId)
    }

    @ApiOperation(value = "Cancel a query")
    @Path("/cancel")
    @POST
    fun cancelQuery(@Named("userContext") auth: ProjectAuth, taskId: UUID): SuccessMessage {
        if (!auth.isSuperuser) {
            throw MetriqlException(HttpResponseStatus.FORBIDDEN)
        }
        service.cancel(taskId)
        return SuccessMessage.success()
    }
}
