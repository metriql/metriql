package com.metriql.tests

import com.metriql.db.QueryResult
import com.metriql.service.auth.ProjectAuth
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.Warehouse
import java.time.ZoneId
import kotlin.test.fail

interface TestingServer<C> {
    val config: Warehouse.Config
    val dataSource: DataSource
    fun getQueryRunner(): C
    fun init() {}

    val bridge get() = dataSource.warehouse.bridge

    val auth get() = ProjectAuth.singleProject(ZoneId.of("UTC"))

    fun runQueryFirstRow(query: String): List<Any?>? {
        val task = dataSource.createQueryTask(
            auth,
            QueryResult.QueryStats.QueryInfo.rawSql(query),
            null,
            null,
            null,
            false
        ).runAndWaitForResult()
        if (task.error != null) {
            fail("Error running query: $query \n ${task.error}")
        }

        return if (task.result?.isEmpty() == true) {
            null
        } else {
            task.result?.get(0)
        }
    }
}
