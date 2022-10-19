package com.metriql.report

import com.metriql.report.data.ReportFilters
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.report.sql.SqlReportOptions
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceReportOptions
import io.netty.handler.codec.http.HttpResponseStatus

interface IAdHocService<T : ServiceReportOptions> {
    /**
     * A factory pattern queryTask generator. Given reportOptions, passes options to corresponding service.
     * Service then builds up the query and passes to SQLQueryExecutor to pass the final limits and checks the short-lived
     * cache (~15 minutes). Then passes the query to Warehouses executeTask function and returns the queryTask.
     *
     * @param context A context passed around till arrives to SQLQueryExecutor,
     * a single context managing all fields and view-models. If non passed, it is the root and will create a new fresh context
     * @param reportOptions An abstraction of service options.
     * @param reportFilters Filters from reports.
     * @return a queryTask with QueryResult on result and QueryStats on poll
     * */
    fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: T,
        reportFilters: ReportFilters = listOf()
    ): RenderedQuery

    fun getUsedModels(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: T): Set<ModelName> {
        return setOf()
    }

    fun generateMaterializeQuery(
        projectId: String,
        context: IQueryGeneratorContext,
        modelName: ModelName,
        materializeName: String,
        materialize: SegmentationRecipeQuery.SegmentationMaterialize
    ): Pair<Model.Target.TargetValue.Table, String> {
        throw MetriqlException("This report type doesn't support materialization", HttpResponseStatus.NOT_IMPLEMENTED)
    }

    data class RenderedQuery(val query: String, val postProcessors: List<PostProcessor> = listOf(), val queryOptions: SqlReportOptions.QueryOptions? = null)
}
