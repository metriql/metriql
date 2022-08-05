package com.metriql.warehouse.bigquery

import com.google.cloud.bigquery.BigQuery
import com.metriql.tests.TestingServer
import com.metriql.util.JsonHelper

object TestingEnvironmentBigQuery : TestingServer<BigQuery> {

    override val config = JsonHelper.read(
        System.getenv("METRIQL_TEST_BIGQUERY_CREDENTIALS")
            ?: throw IllegalStateException("METRIQL_BIGQUERY_CREDENTIALS environment variable is required to run the tests"),
        BigQueryWarehouse.BigQueryConfig::class.java
    )!!

    override val dataSource = BigQueryDataSource(config)

    override fun getQueryRunner(): BigQuery = dataSource.bigQuery

    @Synchronized
    override fun init() {
        // delete if exists
//        dataSource.bigQuery.delete(DatasetId.of(config.project, config.dataset), BigQuery.DatasetDeleteOption.deleteContents())
//        dataSource.bigQuery.create(DatasetInfo.newBuilder(config.project, config.dataset).build())
    }
}
