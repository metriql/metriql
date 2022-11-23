package com.metriql.service.jdbc

import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.report.sql.SqlQuery
import com.metriql.report.sql.SqlReportType
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.dataset.IDatasetService
import com.metriql.warehouse.metriql.CatalogFile
import com.metriql.warehouse.metriql.ExternalConnectorFactory
import io.trino.LocalTrinoQueryRunner
import io.trino.MetriqlConnectorFactory
import io.trino.MetriqlConnectorFactory.Companion.METRIQL_AUTH_PROPERTY
import io.trino.MetriqlConnectorFactory.Companion.QUERY_TYPE_PROPERTY
import io.trino.MetriqlMetadata.Companion.getMetriqlType
import io.trino.execution.warnings.WarningCollector
import io.trino.server.HttpRequestSessionContext
import io.trino.spiller.NodeSpillConfig
import io.trino.sql.analyzer.FeaturesConfig
import io.trino.sql.planner.plan.OutputNode
import io.trino.testing.TestingGroupProvider
import java.util.logging.Level
import java.util.logging.Logger

const val QUERY_TYPE = "QUERY_TYPE"

class LightweightQueryRunner(private val datasetService: IDatasetService) {
    val runner: LocalTrinoQueryRunner by lazy {
        var internalRunner = LocalTrinoQueryRunner(FeaturesConfig(), NodeSpillConfig())
        internalRunner.addSystemProperty(QUERY_TYPE_PROPERTY)
        internalRunner.addSystemProperty(METRIQL_AUTH_PROPERTY)
        internalRunner.createCatalog("metriql", MetriqlConnectorFactory(internalRunner.nodeManager, datasetService), mapOf())
        // internalRunner.createCatalog("tpch", TpchConnectorFactory(), mapOf("tpch.produce-pages" to "true"))
        internalRunner
    }

    val groupProviderManager = TestingGroupProvider()

    fun createTask(auth: ProjectAuth, sessionContext: HttpRequestSessionContext, sql: String): QueryTask {
        return TrinoQueryTask(sessionContext, sql, auth)
    }

    fun start(catalogs: CatalogFile.Catalogs?) {
        catalogs?.forEach { name, catalog ->
            runner.createCatalog(name, ExternalConnectorFactory(runner.nodeManager, name, catalog), mapOf())
        }
        runner
    }

    inner class TrinoQueryTask(private val sessionContext: HttpRequestSessionContext, val sql: String, auth: ProjectAuth) :
        QueryTask(auth.projectId, auth.userId, auth.source, false) {
        override fun getStats(): QueryResult.QueryStats {
            val state = when (status) {
                Status.QUEUED -> QueryResult.QueryStats.State.QUEUED
                Status.RUNNING -> QueryResult.QueryStats.State.RUNNING
                Status.CANCELED -> QueryResult.QueryStats.State.FINISHED
                Status.FINISHED -> QueryResult.QueryStats.State.FINISHED
                Status.FAILED -> QueryResult.QueryStats.State.FINISHED
            }
            val info = QueryResult.QueryStats.QueryInfo(SqlReportType, SqlQuery(sql, null, null, null), sql)
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
                setResult(QueryResult.errorResult(e.message ?: fallbackErrorMessage), failed = true)
            }
        }
    }

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)
    }
}
