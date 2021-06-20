package com.metriql.service.jdbc

import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.Model
import com.metriql.warehouse.spi.WarehouseAuth
import io.trino.LocalQueryRunner
import io.trino.MetriqlConnectorFactory
import io.trino.MetriqlMetadata.Companion.getMetriqlType
import io.trino.execution.warnings.WarningCollector
import io.trino.spiller.NodeSpillConfig
import io.trino.sql.analyzer.FeaturesConfig
import io.trino.sql.planner.plan.OutputNode
import org.rakam.server.http.RakamHttpRequest
import java.util.logging.Level
import java.util.logging.Logger

const val QUERY_TYPE = "QUERY_TYPE"

class LightweightQueryRunner(private val models: List<Model>) {
    lateinit var runner: LocalQueryRunner

    fun createTask(auth: ProjectAuth, request: RakamHttpRequest, sql: String): QueryTask {
        return TrinoQueryTask(request, sql, auth.warehouseAuth())
    }

    fun start() {
        logger.info("Starting metadata query runner")
        this.runner = LocalQueryRunner(FeaturesConfig(), NodeSpillConfig())
        runner.createCatalog("metriql", MetriqlConnectorFactory(runner.nodeManager, models), mapOf())
    }

    inner class TrinoQueryTask(val request: RakamHttpRequest, val sql: String, auth: WarehouseAuth) : QueryTask(auth.projectId, auth.userId, false) {
        override fun getStats(): QueryResult.QueryStats {
            return QueryResult.QueryStats(status.name)
        }

        override fun run() {
            try {
                val query = runner.executeWithPlan(request, sql, getId(), WarningCollector.NOOP)
                val columnNames = (query.queryPlan?.root as? OutputNode)?.columnNames
                val columns =
                    query.materializedResult?.types?.mapIndexed { index, it -> QueryResult.QueryColumn(columnNames?.get(index) ?: "_col$index", index, getMetriqlType(it)) }
                        ?: listOf()
                val data = query.materializedResult?.materializedRows?.map { it.fields } ?: listOf()
                setResult(QueryResult(columns, data, null, mapOf(QUERY_TYPE to query.updateType), query.responseHeaders))
            } catch (e: Exception) {
                val fallbackErrorMessage = "Error running metadata query: ${e.message}"
                logger.log(Level.WARNING, fallbackErrorMessage, e)
                setResult(QueryResult.errorResult(e.message ?: fallbackErrorMessage))
            }
        }
    }

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)
    }
}
