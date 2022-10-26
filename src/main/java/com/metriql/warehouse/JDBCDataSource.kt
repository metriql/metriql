package com.metriql.warehouse

import com.metriql.db.FieldType
import com.metriql.db.QueryResult
import com.metriql.report.QueryTask
import com.metriql.service.audit.MetriqlEvents
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.SQLRenderable
import com.metriql.service.model.Dataset
import com.metriql.util.JdbcUtil
import com.metriql.util.JdbcUtil.fromGenericJDBCTypeFieldType
import com.metriql.util.MetriqlEventBus
import com.metriql.util.MetriqlException
import com.metriql.util.MetriqlExceptions
import com.metriql.util.Scheduler
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.DatabaseName
import com.metriql.warehouse.spi.SchemaName
import com.metriql.warehouse.spi.TableName
import com.metriql.warehouse.spi.TableSchema
import com.metriql.warehouse.spi.Warehouse
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.netty.handler.codec.http.HttpResponseStatus
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLTransientConnectionException
import java.sql.Statement
import java.time.ZoneId
import java.util.HashMap
import java.util.Properties
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

abstract class JDBCDataSource(
    override val config: Warehouse.Config,
    private val tableTypes: Array<String>,
    private val usePool: Boolean,
    private val supportsCrossDatabaseQueries: Boolean,
    private val defaultDatabase: String?,
    private val defaultSchema: String?, // Not every database supports schema (i.e mysql)
) : DataSource {

    protected abstract val dataSourceProperties: Properties

    open fun getPoolConfig(dataSourceProperties: Properties): HikariConfig {
        val hikariConfig = HikariConfig(dataSourceProperties)
        hikariConfig.scheduledExecutor = Scheduler.executor
        hikariConfig.initializationFailTimeout = 10000
        hikariConfig.poolName = "metriql-project-${dataSourceProperties["jdbcUrl"]}"
        hikariConfig.minimumIdle = 0
        hikariConfig.maxLifetime = TimeUnit.HOURS.toMillis(2)
        hikariConfig.maximumPoolSize = 25
        return hikariConfig
    }

    open fun openConnection(timezone: ZoneId? = null): Connection {
        val customDataSourceProperties = getPropertiesForSession(timezone)
        return try {
            pool.computeIfAbsent(customDataSourceProperties) { HikariDataSource(getPoolConfig(customDataSourceProperties)) }.connection
        } catch (e: SQLException) {
            if (e is SQLTransientConnectionException && e.message?.contains("Connection is not available, request timed out after") == true) {
                val maxConnectionCount = pool[customDataSourceProperties]?.maximumPoolSize
                if (maxConnectionCount != null) {
                    throw RuntimeException("Maximum $maxConnectionCount concurrent queries are allowed, please slow down or request more slots from your administrator", e)
                }
            }

            val message = e.cause?.toString()
            throw RuntimeException("Unable to connect: ${e.message} ${if (message != null) "Reason: $message" else ""}", e)
        }
    }

    open fun getPropertiesForSession(timezone: ZoneId?) = dataSourceProperties

    // When JDBCType is not enough to parse warehouse specific types.
    open fun getFieldType(sqlType: Int, dbType: String): FieldType? = null

    open fun getColumnValue(auth: ProjectAuth, conn: Connection, obj: Any, type: FieldType): Any? = null

    override fun listDatabaseNames(): List<DatabaseName> {
        if (!supportsCrossDatabaseQueries) {
            return listOfNotNull(defaultDatabase)
        }
        return openConnection().use { connection ->
            val meta = connection.metaData
            val rs = meta.catalogs
            val databases = mutableListOf<String>()
            while (rs.next()) {
                databases.add(rs.getString(1))
            }
            databases
        }
    }

    private fun readResultSetForMetadata(rs: ResultSet, columns: MutableMap<String, MutableList<TableSchema.Column>>) {
        while (rs.next()) {
            val tableName = rs.getString("TABLE_NAME")
            val columnName = rs.getString("COLUMN_NAME")
            val sqlType = rs.getInt("DATA_TYPE")
            val dbType = rs.getString("TYPE_NAME")
            // try parsing first using db-type
            val fieldType = getFieldType(sqlType, dbType) ?: fromGenericJDBCTypeFieldType(sqlType)
            if (columns[tableName] == null) {
                columns[tableName] = mutableListOf()
            }
            columns[tableName]?.add(TableSchema.Column(columnName, null, dbType, fieldType, null))
        }
    }

    override fun listSchema(
        database: String?,
        schema: String?,
        tables: Collection<String>?,
    ): List<TableSchema> {
        fun getColumns(ofTable: String?): HashMap<String, MutableList<TableSchema.Column>> {
            openConnection().use { connection ->
                if (supportsCrossDatabaseQueries) {
                    connection.catalog = database ?: defaultDatabase
                }
                if (defaultSchema != null) {
                    connection.schema = schema ?: defaultSchema
                }
                val columns = HashMap<String, MutableList<TableSchema.Column>>()
                val rs = connection.metaData.getColumns(
                    database ?: defaultDatabase,
                    schema ?: defaultSchema,
                    ofTable ?: "%",
                    "%"
                )
                readResultSetForMetadata(rs, columns)
                return columns
            }
        }

        return if (tables?.isNotEmpty() == true) {
            val tableFetcherThreads = Executors.newWorkStealingPool(5)
            val tableFutures = tables.map { table ->
                Callable {
                    getColumns(table).map { (tableName, columns) ->
                        TableSchema(tableName, null, columns)
                    }
                }
            }
            tableFetcherThreads.invokeAll(tableFutures).map { it.get() }.flatten()
        } else {
            getColumns(null).map { (tableName, columns) ->
                TableSchema(tableName, null, columns)
            }
        }
    }

    override fun listSchemaNames(database: String?): List<SchemaName> {
        if (defaultSchema == null) {
            throw IllegalStateException("This warehouse does not support schema")
        }
        return openConnection().use { connection ->
            val meta = connection.metaData
            val rs = meta.getSchemas(database ?: defaultDatabase, "%")
            val schemas = mutableListOf<String>()
            while (rs.next()) {
                if (schemas.size >= 250) {
                    break
                }
                schemas.add(rs.getString(1))
            }
            schemas
        }
    }

    override fun getTableSchema(database: String?, schema: String?, table: String): TableSchema {
        openConnection().use { connection ->
            val meta = connection.metaData
            val finalDatabase = if (meta.storesUpperCaseIdentifiers()) database?.uppercase() else database
            val finalSchema = if (meta.storesUpperCaseIdentifiers()) schema?.uppercase() else schema
            val finalTable = if (meta.storesUpperCaseIdentifiers()) table?.uppercase() else table
            val resultSet = meta.getColumns(finalDatabase ?: defaultDatabase, finalSchema ?: defaultSchema, finalTable, "%")

            val columns = HashMap<String, MutableList<TableSchema.Column>>()
            readResultSetForMetadata(resultSet, columns)

            if (columns.isEmpty()) {
                throw MetriqlException("Table $table not found in '${database ?: defaultDatabase}.${schema ?: defaultSchema}'", HttpResponseStatus.NOT_FOUND)
            }
            return columns.map { (tableName, columns) ->
                TableSchema(tableName, null, columns)
            }.first()
        }
    }

    override fun getTableSchema(sql: String): TableSchema {
        openConnection().use { connection ->
            val preparedStatement = connection.prepareStatement(sql)
            preparedStatement.maxRows = 0
            preparedStatement.execute()
            val metaData = preparedStatement.metaData
            val columns = (1..metaData.columnCount)
                .map {
                    val sqlType = metaData.getColumnType(it)
                    val dbType = metaData.getColumnTypeName(it)
                    val name = metaData.getColumnName(it)
                    val label = metaData.getColumnLabel(it)
                    val fieldType = getFieldType(sqlType, dbType) ?: fromGenericJDBCTypeFieldType(sqlType)
                    if (fieldType == null) {
                        logger.warning("Unable to parse type $name for JDBC url ${this.dataSourceProperties["jdbcUrl"]} ")
                    }
                    TableSchema.Column(name, null, dbType, fieldType, label)
                }
            return TableSchema("derived", null, columns)
        }
    }

    override fun listTableNames(database: String?, schema: String?): List<TableName> {
        openConnection().use { connection ->
            val meta = connection.metaData
            val resultSet = meta.getTables(database ?: defaultDatabase, schema ?: defaultSchema, "%", tableTypes)

            val tableNames = mutableListOf<String>()
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("TABLE_NAME"))
            }
            return tableNames
        }
    }

    override fun sqlReferenceForTarget(
        target: Dataset.Target,
        aliasName: String,
        renderSQL: (SQLRenderable) -> String,
    ): String {
        return when (target.value) {
            is Dataset.Target.TargetValue.Sql -> renderSQL(target.value.sql)
            is Dataset.Target.TargetValue.Table -> {
                val (databaseName, schemaName, table) = target.value

                val targetBuilder = mutableListOf<String>()
                if (supportsCrossDatabaseQueries && databaseName != null) {
                    targetBuilder.add(warehouse.bridge.quoteIdentifier(databaseName))
                }
                if (defaultSchema != null && schemaName != null) {
                    targetBuilder.add(warehouse.bridge.quoteIdentifier(schemaName))
                }
                targetBuilder.add(warehouse.bridge.quoteIdentifier(table))
                targetBuilder.joinToString(".")
            }
        } + "AS ${warehouse.bridge.quoteIdentifier(aliasName)}"
    }

    override fun fillDefaultsToTarget(target: Dataset.Target): Dataset.Target {
        return when (target.value) {
            is Dataset.Target.TargetValue.Sql -> target
            is Dataset.Target.TargetValue.Table -> {
                target.copy(
                    value = target.value.copy(
                        database = target.value.database ?: defaultDatabase,
                        schema = target.value.schema ?: defaultSchema
                    )
                )
            }
        }
    }

    override fun connectionTest(userId: Int): Boolean {
        return try {
            openConnection().use {
                // it.isValid(10000) Can also use this ?
                if (supportsCrossDatabaseQueries) {
                    it.catalog = defaultDatabase
                }
                if (defaultSchema.isNullOrBlank()) {
                    it.metaData.catalogs.next()
                } else {
                    it.schema = defaultSchema
                    it.metaData.getSchemas(defaultDatabase, defaultSchema).next()
                }
            }
        } catch (e: Exception) {
            throw MetriqlExceptions.SYSTEM_EXCEPTION_FROM_CAUSE.exceptionFromObject(e)
        }
    }

    companion object {
        val pool = ConcurrentHashMap<Properties, HikariDataSource>()

        fun syncStats(isRunning: Boolean, query: QueryResult.QueryStats.QueryInfo): QueryResult.QueryStats {
            return if (isRunning) {
                QueryResult.QueryStats(QueryResult.QueryStats.State.RUNNING, query, nodes = 1, percentage = 100.0)
            } else {
                QueryResult.QueryStats(QueryResult.QueryStats.State.FINISHED, query, nodes = 1)
            }
        }

        fun getErrorQueryResult(auth: ProjectAuth, e: Exception, query: QueryResult.QueryStats.QueryInfo, ignoredExceptionCodes: List<String>): QueryResult {
            return when (e) {
                is SQLException -> {
                    if (!ignoredExceptionCodes.isNullOrEmpty() &&
                        !ignoredExceptionCodes.map { ignoredCode -> e.sqlState?.startsWith(ignoredCode) == true }.reduce { c1, c2 -> c1 || c2 }
                    ) {
                        MetriqlEventBus.publish(MetriqlEvents.InternalException(e as Throwable, auth.userId, auth.projectId))
                    }
                    QueryResult.errorResult(QueryResult.QueryError.create(e), query)
                }
                else -> {
                    QueryResult.errorResult(QueryResult.QueryError.create(e))
                }
            }
        }
    }

    fun createSyncQueryTask(
        auth: ProjectAuth,
        query: QueryResult.QueryStats.QueryInfo,
        defaultSchema: String?,
        defaultDatabase: String?,
        limit: Int?,
        ignoredErrorCodes: List<String> = listOf(),
    ): QueryTask {
        return object : QueryTask(auth.projectId, auth.userId, auth.source, false) {
            private var resultSet: ResultSet? = null
            private lateinit var statement: Statement

            override fun run() {
                try {
                    openConnection(auth.timezone).use { conn ->
                        statement = conn.createStatement()

                        setupConnection(auth, statement, defaultDatabase, defaultSchema, limit)

//                        resultSet = statement.unwrap(SnowflakeStatement::class.java).executeAsyncQuery(query)
                        markAsRunning()
                        resultSet = statement.executeQuery(query.compiledQuery)

                        val value = JdbcUtil.toQueryResult(
                            resultSet!!,
                            { sqlType, dbType ->
                                getFieldType(sqlType, dbType) ?: fromGenericJDBCTypeFieldType(sqlType) ?: FieldType.UNKNOWN
                            },
                            { type: FieldType, obj ->
                                getColumnValue(auth, conn, obj, type)
                            },
                            auth.timezone
                        )

                        setResult(value)
                    }
                } catch (e: Exception) {
                    MetriqlEventBus.publish(MetriqlEvents.UnhandledTaskException(e, this))
                    setResult(getErrorQueryResult(auth, e, query, ignoredErrorCodes), failed = true)
                }
            }

            override fun getStats(): QueryResult.QueryStats {
//                val unwrap = resultSet!!.unwrap(SnowflakeResultSet::class.java)
                return syncStats(!status.isDone, query)
            }
        }
    }

    open fun setupConnection(auth: ProjectAuth, statement: Statement, defaultDatabase: String?, defaultSchema: String?, limit: Int?) {
        val conn = statement.connection

        if (conn.catalog != defaultDatabase) {
            conn.catalog = defaultDatabase
        }

        if (conn.schema != defaultSchema) {
            conn.schema = defaultSchema
        }

        if (statement.maxRows != limit) {
            statement.maxRows = limit ?: WarehouseQueryTask.DEFAULT_LIMIT
        }
    }

    private val logger = Logger.getLogger(this::class.java.name)
}
