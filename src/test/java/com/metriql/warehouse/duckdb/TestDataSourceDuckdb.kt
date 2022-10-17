package com.metriql.warehouse.duckdb

import com.google.common.io.Resources
import com.metriql.tests.JdbcTestDataSource
import org.testng.annotations.Test

class TestDataSourceDuckdb : JdbcTestDataSource() {
    override val testingServer = TestingEnvironmentEmbeddedDuckdb

    @Test
    fun testName() {
        val filePath = Resources.getResource("duckdb/simple.parquet").file
//        val runQueryFirstRow = testingServer.runQueryFirstRow("SELECT count(*) FROM read_csv_auto('$CSV', header=False)")
        val runQueryFirstRow = testingServer.runQueryFirstRow("COPY '$CSV' TO 'views.parquet' (FORMAT 'PARQUET')")
        println(runQueryFirstRow)
    }

    override fun populate() {
        // no-op
    }

    companion object {
        const val CSV = "/Users/bkabak/Downloads/pageviews-20220801-user.csv"
    }
}
