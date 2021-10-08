package com.metriql.report.sql

import com.google.inject.Inject
import com.metriql.report.IAdHocService
import com.metriql.report.data.ReportFilter
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jdbc.StatementService.Companion.defaultParsingOptions
import com.metriql.service.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.WarehouseQueryTask.Companion.MAX_LIMIT
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.sql.MetriqlSqlFormatter
import io.trino.sql.SqlToSegmentation
import io.trino.sql.parser.SqlParser

class MqlService @Inject constructor(private val reWriter: SqlToSegmentation) : IAdHocService<SqlReportOptions> {
    val parser = SqlParser()

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: SqlReportOptions,
        reportFilters: List<ReportFilter>,
    ): IAdHocService.RenderedQuery {
        val statement = parser.createStatement(reportOptions.query, defaultParsingOptions)
        val compiledQuery = try {
            MetriqlSqlFormatter.formatSql(statement, reWriter, context)
        } catch (e: Exception) {
            throw MetriqlException("Unable to parse query: $e", HttpResponseStatus.BAD_REQUEST)
        }

        val queryOptions = reportOptions.queryOptions?.copy(limit = reportOptions.queryOptions?.limit ?: MAX_LIMIT)
            ?: SqlReportOptions.QueryOptions(MAX_LIMIT, null, null, true)
        return IAdHocService.RenderedQuery(compiledQuery, queryOptions = queryOptions)
    }

    override fun getUsedModels(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: SqlReportOptions): Set<ModelName> = setOf()
}
