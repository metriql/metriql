package com.metriql.warehouse.azure

import com.metriql.warehouse.mssqlserver.MSSQLMetriqlBridge
import com.metriql.warehouse.mssqlserver.MSSQLWarehouse
import com.metriql.warehouse.spi.Warehouse
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import java.sql.DriverManager

object AzureWarehouse : Warehouse<MSSQLWarehouse.MSSQLServerConfig> {

    init {
        DriverManager.registerDriver(SQLServerDriver())
    }

    override val names = Warehouse.Name("azureSql", "null")

    override val configClass = MSSQLWarehouse.MSSQLServerConfig::class.java

    override fun getDataSource(config: MSSQLWarehouse.MSSQLServerConfig) = AzureSQLDataSource(config)

    override val bridge = MSSQLMetriqlBridge
}

class AzureWarehouseProxy : Warehouse<MSSQLWarehouse.MSSQLServerConfig> by AzureWarehouse
