package com.metriql.warehouse.duckdb

import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.JDBCDataSource
import com.metriql.warehouse.postgresql.PostgresqlDataSource
import com.metriql.warehouse.spi.Warehouse
import java.sql.Statement
import java.util.Properties

class DuckdbDataSource(override val config: DuckdbWarehouse.DuckdbConfig, private val readOnly: Boolean = true) : JDBCDataSource(
    config,
    arrayOf("TABLE", "VIEW", "MATERIALIZED VIEW"),
    false,
    false,
    config.db_path,
    null
) {
    override val warehouse: Warehouse<*> = DuckdbWarehouse

    override val dataSourceProperties: Properties by lazy {
        val properties = Properties()
        properties["jdbcUrl"] = "jdbc:duckdb:" + if (config.db_path != null) {
            "//${config.db_path}"
        } else ""
        properties["driverClassName"] = "org.duckdb.DuckDBDriver"
        properties["dataSource.duckdb.read_only"] = readOnly
        properties["dataSource.ApplicationName"] = "metriql"
        config.connectionParameters?.map { (k, v) ->
            properties["dataSource.$k"] = v
        }
        properties
    }

    override fun createQueryTask(
        auth: ProjectAuth,
        query: QueryResult.QueryStats.QueryInfo,
        defaultSchema: String?,
        defaultDatabase: String?,
        limit: Int?,
        isBackgroundTask: Boolean
    ): QueryTask {
        return createSyncQueryTask(
            auth,
            query,
            defaultSchema ?: PostgresqlDataSource.DEFAULT_SCHEMA,
            defaultDatabase ?: config.db_path,
            limit,
            ignoredErrorCodes = listOf("42", "43", "28", "53", "54", "55", "57") // https://www.postgresql.org/docs/9.6/errcodes-appendix.html
        )
    }

    override fun setupConnection(auth: ProjectAuth, statement: Statement, defaultDatabase: String?, defaultSchema: String?, limit: Int?) {
        statement.connection
    }
}
