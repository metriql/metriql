package com.metriql.warehouse.redshift

import com.metriql.warehouse.postgresql.PostgresqlWarehouse
import com.metriql.warehouse.spi.Warehouse
import java.sql.DriverManager

object RedshiftWarehouse : Warehouse<PostgresqlWarehouse.PostgresqlConfig> {
    init {
        DriverManager.registerDriver(com.amazon.redshift.jdbc42.Driver())
    }

    override val names = Warehouse.Name("redshift", "redshift")
    override val configClass = PostgresqlWarehouse.PostgresqlConfig::class.java

    override fun getDataSource(config: PostgresqlWarehouse.PostgresqlConfig) = RedshiftDataSource(config)

    override val bridge = RedshiftMetriqlBridge
}

class RedshiftWarehouseProxy : Warehouse<PostgresqlWarehouse.PostgresqlConfig> by RedshiftWarehouse
