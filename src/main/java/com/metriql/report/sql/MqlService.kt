package com.metriql.report.sql

import com.google.inject.Inject
import com.metriql.report.IAdHocService
import com.metriql.report.ReportFilter
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.service.model.ModelName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.sql.MetriqlSqlFormatter
import io.trino.sql.SqlToSegmentation
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import java.util.logging.Level
import java.util.logging.Logger

class MqlService @Inject constructor(private val modelService: IModelService, private val reWriter: SqlToSegmentation) : IAdHocService<SqlReportOptions> {
    val parser = SqlParser()

    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: SqlReportOptions,
        reportFilters: List<ReportFilter>,
    ): IAdHocService.RenderedQuery {
        val sqlReportOptions = reportOptions as SqlReportOptions

        val models = modelService.list(auth)
        val statement = parser.createStatement(
            sqlReportOptions.query,
            ParsingOptions()
        )
        val compiledQuery = try {
            MetriqlSqlFormatter.formatSql(
                statement,
                reWriter,
                context,
                models
            )
        } catch (e: Exception) {
            logger.log(Level.WARNING, "Unable to parse query", e)
            throw MetriqlException("Unable to parse query: ${e.message}", HttpResponseStatus.BAD_REQUEST)
        }

        return IAdHocService.RenderedQuery(compiledQuery, queryOptions = sqlReportOptions.queryOptions)
    }

    override fun getUsedModels(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: SqlReportOptions): Set<ModelName> = setOf()

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)
    }
}
