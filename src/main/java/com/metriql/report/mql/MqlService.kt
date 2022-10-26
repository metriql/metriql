package com.metriql.report.mql

import com.google.inject.Inject
import com.metriql.report.IAdHocService
import com.metriql.report.data.ReportFilter
import com.metriql.report.sql.SqlQuery
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jdbc.StatementService.Companion.defaultParsingOptions
import com.metriql.service.model.DatasetName
import com.metriql.util.MetriqlException
import com.metriql.warehouse.WarehouseQueryTask.Companion.MAX_LIMIT
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.sql.MetriqlSqlFormatter
import io.trino.sql.ParameterUtils
import io.trino.sql.SqlToSegmentation
import io.trino.sql.parser.SqlParser
import io.trino.sql.tree.Expression
import io.trino.sql.tree.NodeRef
import io.trino.sql.tree.Parameter
import java.util.logging.Level
import java.util.logging.Logger

class MqlService @Inject constructor(private val reWriter: SqlToSegmentation) : IAdHocService<MqlQuery> {
    override fun renderQuery(
        auth: ProjectAuth,
        context: IQueryGeneratorContext,
        reportOptions: MqlQuery,
        reportFilters: ReportFilter?,
    ): IAdHocService.RenderedQuery {
        val statement = parser.createStatement(reportOptions.query, defaultParsingOptions)
        val parameterMap: Map<NodeRef<Parameter>, Expression> = ParameterUtils.parameterExtractor(statement, reportOptions.variables ?: listOf())

        val compiledQuery = try {
            MetriqlSqlFormatter.formatSql(statement, reWriter, context, parameterMap)
        } catch (e: MetriqlException) {
            throw e
        } catch (e: Exception) {
            logger.log(Level.WARNING, "Unable to parse MQL query", e)
            throw MetriqlException("Unable to parse query: $e", HttpResponseStatus.BAD_REQUEST)
        }

        val opt = reportOptions.queryOptions?.copy(limit = reportOptions.queryOptions?.limit ?: MAX_LIMIT)
            ?: MqlQuery.QueryOptions(MAX_LIMIT, null, null, true)
        val queryOptions = SqlQuery.QueryOptions(opt.limit, opt.defaultDatabase, opt.defaultSchema, opt.useCache)
        return IAdHocService.RenderedQuery(compiledQuery, queryOptions = queryOptions)
    }

    override fun getUsedDatasets(auth: ProjectAuth, context: IQueryGeneratorContext, reportOptions: MqlQuery): Set<DatasetName> = setOf()

    companion object {
        val parser = SqlParser()
        private val logger = Logger.getLogger(this::class.java.name)
    }
}
