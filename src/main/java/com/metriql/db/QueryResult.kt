package com.metriql.db

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.metriql.util.MetriqlException
import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger

data class QueryResult @JsonCreator constructor(
    val metadata: List<QueryColumn>?,
    val result: List<List<Any?>>?,
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    val error: QueryError?,
    var properties: Map<String, Any>?,
    var responseHeaders: Map<String, String>? = null
) {

    constructor(metadata: List<QueryColumn>, result: List<List<Any?>>) : this(metadata, result, null, null)
    constructor(metadata: List<QueryColumn>, result: List<List<Any?>>, properties: Map<String, Any>?) : this(
        metadata,
        result,
        null,
        properties
    )

    @Synchronized
    fun setProperty(key: String, value: Any) {
        val map = ConcurrentHashMap<String, Any>()
        if (properties != null) {
            map.putAll(properties!!)
        }
        map[key] = value
        properties = map
    }

    @Synchronized
    fun setQueryProperties(query: String, limit: Int) {
        setProperty(QUERY, query)
        setProperty(QUERY_LIMIT, limit)
    }

    data class QueryColumn(
        val name: String,
        val position: Int,
        val type: FieldType?,
        val dbType: String? = null
    )

    data class QueryError(
        val message: String?,
        val sqlState: String?,
        val errorCode: Int?,
        val errorLine: Int?,
        val charPositionInLine: Int?
    ) {

        companion object {
            private val logger = Logger.getLogger(this::class.java.name)

            fun create(message: String): QueryError {
                return QueryError(message, null, null, null, null)
            }

            fun create(e: Throwable): QueryError {
                val message = when (e) {
                    is MetriqlException -> e.errors.joinToString(", ") { it.title }
                    else -> {
                        logger.log(Level.SEVERE, "Query Error", e)
                        e.message ?: e.cause?.message ?: "An unknown error occurred, please contact the administrator"
                    }
                }
                return QueryError(message, null, null, null, null)
            }

            fun create(ex: SQLException): QueryError {
                val message = if (ex.cause != null) {
                    "${ex.message}, Cause: ${ex.cause?.message ?: "Unknown"}"
                } else {
                    ex.message
                }
                return QueryError(message ?: ex.toString(), ex.sqlState, ex.errorCode, null, null)
            }
        }
    }

    data class QueryStats(
        val state: State,
        val query: String?,
        val nodes: Int? = null,
        val percentage: Double? = null,
        val elapsedTimeMillis: Long? = null,
        val totalBytes: Long? = null,
        val processedBytes: Long? = null,
    ) {

        enum class State {
            QUEUED,
            FINISHED,
            RUNNING,
            CONNECTING_TO_DATABASE
        }
    }

    companion object {
        const val QUERY = "query"
        const val QUERY_LIMIT = "limit"

        private val EMPTY = QueryResult(listOf(), listOf())

        fun errorResult(message: String): QueryResult {
            return QueryResult(null, null, QueryError(message, null, null, null, null), null)
        }

        fun errorResult(error: QueryError): QueryResult {
            return QueryResult(null, null, error, null)
        }

        fun errorResult(error: QueryError, query: String): QueryResult {
            return QueryResult(null, null, error, mapOf("query" to query))
        }

        fun empty(): QueryResult {
            return EMPTY
        }

        fun toMap(metadata: List<QueryColumn>, result: List<List<Any?>>): List<Map<String, Any?>> {
            return result.map { row ->
                val obj = mutableMapOf<String, Any?>()
                metadata.forEachIndexed { idx, col ->
                    obj[col.name] = row[idx]
                }
                obj
            }
        }
    }
}
