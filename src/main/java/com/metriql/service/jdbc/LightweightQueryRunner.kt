package com.metriql.service.jdbc

import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.report.ReportType
import com.metriql.report.sql.SqlReportOptions
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.IModelService
import com.metriql.warehouse.spi.WarehouseAuth
import io.trino.LocalQueryRunner
import io.trino.MetriqlConnectorFactory
import io.trino.MetriqlMetadata.Companion.getMetriqlType
import io.trino.execution.warnings.WarningCollector
import io.trino.server.HttpRequestSessionContext
import io.trino.spiller.NodeSpillConfig
import io.trino.sql.analyzer.FeaturesConfig
import io.trino.sql.planner.plan.OutputNode
import java.util.logging.Level
import java.util.logging.Logger

const val QUERY_TYPE = "QUERY_TYPE"

class LightweightQueryRunner(private val modelService: IModelService) {
    val runner: LocalQueryRunner by lazy {
        var internalRunner = LocalQueryRunner(FeaturesConfig(), NodeSpillConfig())
        internalRunner.createCatalog("metriql", MetriqlConnectorFactory(internalRunner.nodeManager, modelService), mapOf())
        // internalRunner.createCatalog("tpch", TpchConnectorFactory(), mapOf("tpch.produce-pages" to "true"))
        internalRunner
    }

    fun createTask(auth: ProjectAuth, sessionContext: HttpRequestSessionContext, sql: String): QueryTask {
        return TrinoQueryTask(sessionContext, sql, auth.warehouseAuth())
    }

    fun start() {
        runner
    }

    inner class TrinoQueryTask(private val sessionContext: HttpRequestSessionContext, val sql: String, auth: WarehouseAuth) :
        QueryTask(auth.projectId, auth.userId, auth.source, false) {
        override fun getStats(): QueryResult.QueryStats {
            val state = when (status) {
                Status.QUEUED -> QueryResult.QueryStats.State.QUEUED
                Status.RUNNING -> QueryResult.QueryStats.State.RUNNING
                Status.CANCELED -> QueryResult.QueryStats.State.FINISHED
                Status.FINISHED -> QueryResult.QueryStats.State.FINISHED
            }
            val info = QueryResult.QueryStats.QueryInfo(ReportType.SQL, SqlReportOptions(sql, null, null, null), sql)
            return QueryResult.QueryStats(state, info)
        }

        override fun run() {
            try {
                val query = runner.executeWithPlan(getId(), sessionContext, sql, WarningCollector.NOOP)
                val columnNames = (query.queryPlan?.root as? OutputNode)?.columnNames
                val columns =
                    query.materializedResult?.types?.mapIndexed { index, it ->
                        QueryResult.QueryColumn(columnNames?.get(index) ?: "_col$index", index, getMetriqlType(it))
                    } ?: listOf()
                val data = query.materializedResult?.materializedRows?.map { it.fields } ?: listOf()
                setResult(QueryResult(columns, data, null, mapOf(QUERY_TYPE to query.updateType), query.responseHeaders))
            } catch (e: Exception) {
                val fallbackErrorMessage = "Error running metadata query"
                logger.log(Level.WARNING, fallbackErrorMessage, e)
                setResult(QueryResult.errorResult(e.message ?: fallbackErrorMessage))
            }
        }
    }

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)
    }
}
