package com.metriql.warehouse.snowflake

import com.metriql.service.model.Model
import com.metriql.tests.JdbcTestDataSource
import org.testng.annotations.Test
import kotlin.test.assertEquals

class TestDataSourceSnowflake : JdbcTestDataSource() {
    override val testingServer = TestingEnvironmentSnowflake

    @Test
    override fun `test fill defaults`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledModelTarget = testingServer.dataSource.fillDefaultsToTarget(modelTarget).value as Model.Target.TargetValue.Table
        assertEquals(filledModelTarget.database, "DEMO_DB")
        assertEquals("RAKAM_TEST", filledModelTarget.schema)
    }

    @Test
    override fun `test generate sql reference`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table("a", "b", "c"))
        val sqlTarget = testingServer.dataSource.sqlReferenceForTarget(modelTarget, "model") { "" }
        assertEquals("a.b.c AS model", sqlTarget)
    }
}
