package com.metriql.report.sql

import com.metriql.auth.ProjectAuth
import com.metriql.model.ModelName
import com.metriql.report.IAdHocService
import com.metriql.report.ReportFilter
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext

class SqlService : IAdHocService<SqlReportOptions> {

    override fun renderQuery(
        auth: ProjectAuth,
        queryContext: IQueryGeneratorContext,
        reportOptions: SqlReportOptions,
        reportFilters: List<ReportFilter>,
    ): IAdHocService.RenderedQuery {
        val sqlReportOptions = reportOptions as SqlReportOptions

        val compiledSql = queryContext.renderSQL(
            sqlReportOptions.query, null,
            dateRange = ReportFilter.extractDateRangeForEventTimestamp(reportFilters)
        )

        return IAdHocService.RenderedQuery(compiledSql, queryOptions = sqlReportOptions.queryOptions)
    }

    override fun getUsedModels(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: SqlReportOptions): Set<ModelName> = setOf()
}
