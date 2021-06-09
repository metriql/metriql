package com.metriql

import com.metriql.db.QueryResult
import com.metriql.report.Recipe
import com.metriql.service.model.Model
import com.metriql.service.task.Task
import org.rakam.server.http.HttpService
import java.util.concurrent.CompletableFuture

abstract class MetriqlApiService<T> : HttpService() {
    abstract fun metadata(context: T): List<Model>
    abstract fun sql(context: T, query: QueryHttpService.Query): String
    abstract fun query(
        context: T,
        query: QueryHttpService.Query,
        useCache: Boolean?,
        initialWaitInSeconds: Long?
    ): CompletableFuture<Task.TaskTicket<QueryResult>>
}
