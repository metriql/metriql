package com.metriql.report

import com.metriql.report.data.ReportFilter
import com.metriql.report.segmentation.SegmentationMaterialize
import com.metriql.report.sql.SqlQuery
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.Dataset
import com.metriql.service.dataset.DatasetName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.ServiceQuery
import io.netty.handler.codec.http.HttpResponseStatus
import kotlin.reflect.KClass

interface IAdHocService<T : ServiceQuery> {

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
        reportFilters: ReportFilter? = null
    ): RenderedQuery

    fun getUsedDatasets(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: T): Set<DatasetName> {
        return setOf()
    }

    fun generateMaterializeQuery(
        projectId: String,
        context: IQueryGeneratorContext,
        datasetName: DatasetName,
        materializeName: String,
        materialize: SegmentationMaterialize
    ): Pair<Dataset.Target.TargetValue.Table, String> {
        throw MetriqlException("This report type doesn't support materialization", HttpResponseStatus.NOT_IMPLEMENTED)
    }

    data class RenderedQuery(val query: String, val postProcessors: List<PostProcessor> = listOf(), val queryOptions: SqlQuery.QueryOptions? = null, val target: KClass<out QueryTaskGenerator> = SqlQueryTaskGenerator::class)
}
