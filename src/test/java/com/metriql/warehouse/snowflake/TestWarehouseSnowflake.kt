package com.metriql.warehouse.snowflake

import com.metriql.service.model.Model
import com.metriql.tests.JdbcTestWarehouse
import org.testng.annotations.Test
import kotlin.test.assertEquals

class TestWarehouseSnowflake : JdbcTestWarehouse() {
    override val testingServer = TestingEnvironmentSnowflake

    @Test
    override fun `test fill defaults`() {
        val modelTarget = Model.Target(Model.Target.Type.TABLE, Model.Target.TargetValue.Table(null, null, "dumb_table"))
        val filledModelTarget = testingServer.dataSource.fillDefaultsToTarget(modelTarget).value as Model.Target.TargetValue.Table
        assertEquals(filledModelTarget.database, "DEMO_DB")
        assertEquals("RAKAM_TEST", filledModelTarget.schema)
    }
}
