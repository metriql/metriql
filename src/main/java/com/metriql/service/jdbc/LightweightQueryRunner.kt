package com.metriql.service.jdbc

import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.model.Model
import com.metriql.service.task.TaskQueueService
import com.metriql.warehouse.spi.WarehouseAuth
import io.trino.LocalQueryRunner
import io.trino.MetriqlConnectorFactory
import io.trino.MetriqlMetadata.Companion.getMetriqlType
import io.trino.Session
import io.trino.metadata.SessionPropertyManager
import io.trino.spi.QueryId
import io.trino.spi.security.Identity
import java.util.logging.Level
import java.util.logging.Logger

class LightweightQueryRunner(private val models: List<Model>) {
    val runner: LocalQueryRunner by lazy {
        logger.info("Starting metadata query runner")
        val session = Session.builder(SessionPropertyManager())
            .setQueryId(QueryId("test"))
            .setIdentity(Identity.ofUser("emre"))
            .setSource("test")
            .setCatalog("metriql")
            .setSchema("public")
            .build()

        val runner = LocalQueryRunner.builder(session).build()
        runner.createCatalog("metriql", MetriqlConnectorFactory(models), mapOf())
        runner
    }

    fun createTask(auth: ProjectAuth, sql: String): QueryTask {
        return TrinoQueryTask(sql, auth.warehouseAuth())
    }

    inner class TrinoQueryTask(
        val sql: String,
        auth: WarehouseAuth
    ) : QueryTask(auth.projectId, auth.userId, false) {
        override fun getStats(): QueryResult.QueryStats {
            return QueryResult.QueryStats(status.name)
        }

        override fun run() {
            try {
                val query = runner.executeWithPlan(sql)
                val columnNames = query.queryPlan.root.outputSymbols
                val columns = query.materializedResult.types.mapIndexed { index, it -> QueryResult.QueryColumn(columnNames[index].name, index, getMetriqlType(it)) }
                val data = query.materializedResult.materializedRows.map { it.fields }
                setResult(QueryResult(columns, data))
            } catch (e: Exception) {
                val fallbackErrorMEssage = "Error running metadata query"
                logger.log(Level.SEVERE, fallbackErrorMEssage, e)
                setResult(QueryResult.errorResult(e.message ?: fallbackErrorMEssage))
            }
        }
    }

    companion object {
        private val logger = Logger.getLogger(this::class.java.name)
    }
}
