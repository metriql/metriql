package com.metriql.report.sql

import com.metriql.report.IAdHocService
import com.metriql.report.data.FilterValue
import com.metriql.report.data.FilterValue.Companion.extractDateRangeForEventTimestamp
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.DatasetName
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class SqlService : IAdHocService<SqlQuery> {

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: SqlQuery,
        reportFilters: FilterValue?,
    ): IAdHocService.RenderedQuery {

        val compiledSql = context.renderSQL(
            reportOptions.query, null, null,
            dateRange = extractDateRangeForEventTimestamp(reportFilters)
        )

        return IAdHocService.RenderedQuery(compiledSql, queryOptions = reportOptions.queryOptions)
    }

    override fun getUsedDatasets(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: SqlQuery): Set<DatasetName> = setOf()
}
