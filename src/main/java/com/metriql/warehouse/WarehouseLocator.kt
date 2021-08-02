package com.metriql.warehouse

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.db.JSONBSerializable
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.Warehouse
import io.netty.handler.codec.http.HttpResponseStatus
import java.util.ServiceLoader

object WarehouseLocator {
    private var services: List<Warehouse<*>> = ServiceLoader.load(Warehouse::class.java).toList()

    @JvmStatic
    fun getWarehouse(slug: String): Warehouse<Warehouse.Config> {
        val service = services.find { it.names.metriql == slug || it.names.dbt == slug }
        val warehouse = service ?: throw MetriqlException("Unknown warehouse: $slug", HttpResponseStatus.BAD_REQUEST)
        return warehouse as Warehouse<Warehouse.Config>
    }

    @JvmStatic
    fun reload() {
        services = ServiceLoader.load(Warehouse::class.java).toList()
    }

    @JvmStatic
    fun getDataSource(warehouseConfig: WarehouseConfig): DataSource {
        val warehouse = getWarehouse(warehouseConfig.type)
        if (warehouse.configClass.isInstance(warehouseConfig.value)) {
            return warehouse.getDataSource(warehouseConfig.value)
        } else {
            throw IllegalArgumentException()
        }
    }
}

class WarehouseConfigJsonDeserializer : JsonDeserializer<WarehouseConfig>() {
    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext?): WarehouseConfig {
        val tree = jp.readValueAsTree<ObjectNode>()
        val type = tree.get("type").textValue()
        val dataSourceClass = WarehouseLocator.getWarehouse(type).configClass
        val convert = try {
            // small hack to support parsing dbt profiles.yml
            if (tree.has("value")) {
                JsonHelper.convert(tree.get("value"), dataSourceClass)
            } else {
                JsonHelper.convert(tree, dataSourceClass)
            }
        } catch (e: Exception) {
            throw MetriqlException("Invalid config: ${e.message}", HttpResponseStatus.BAD_REQUEST)
        }

        return WarehouseConfig(type, convert as Warehouse.Config)
    }
}

@JSONBSerializable
@JsonDeserialize(using = WarehouseConfigJsonDeserializer::class)
data class WarehouseConfig @JsonCreator constructor(val type: String, val value: Warehouse.Config)
