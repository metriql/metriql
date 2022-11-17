package com.metriql.warehouse.snowflake

import com.metriql.service.dataset.Dataset
import com.metriql.tests.JdbcTestDataSource
import org.testng.annotations.Test
import kotlin.test.assertEquals

class TestDataSourceSnowflake : JdbcTestDataSource() {
    override val testingServer = TestingEnvironmentSnowflake

    @Test
    override fun `test fill defaults`() {
        val datasetTarget = Dataset.Target(Dataset.Target.Type.TABLE, Dataset.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledDatasetTarget = testingServer.dataSource.fillDefaultsToTarget(datasetTarget).value as Dataset.Target.TargetValue.Table
        assertEquals(filledDatasetTarget.database, "DEMO_DB")
        assertEquals("RAKAM_TEST", filledDatasetTarget.schema)
    }

    @Test
    override fun `test generate sql reference`() {
        val datasetTarget = Dataset.Target(Dataset.Target.Type.TABLE, Dataset.Target.TargetValue.Table("a", "b", "c"))
        val sqlTarget = testingServer.dataSource.sqlReferenceForTarget(datasetTarget, "model") { "" }
        assertEquals("a.b.c AS model", sqlTarget)
    }
}
