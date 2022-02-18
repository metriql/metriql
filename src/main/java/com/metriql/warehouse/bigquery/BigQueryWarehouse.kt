package com.metriql.warehouse.bigquery

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode
import com.metriql.util.JsonHelper
import com.metriql.warehouse.spi.Warehouse

object BigQueryWarehouse : Warehouse<BigQueryWarehouse.BigQueryConfig> {
    override val names = Warehouse.Name("bigQuery", "bigquery")

    override val bridge = BigQueryMetriqlBridge

    override val configClass = BigQueryConfig::class.java

    override fun getDataSource(config: BigQueryConfig) = BigQueryDataSource(config)

    data class BigQueryConfig(
        val dataset: String,
        val project: String? = null,
        @JsonAlias("serviceAccountJSON")
        @JsonDeserialize(using = StringOrObjectDeserializer::class)
        val keyfile_json: String? = null,
        @JsonAlias("maximum_bytes_billed")
        val maximumBytesBilled: Long? = null,
        @JsonAlias("timeout_seconds")
        val timeoutSeconds: Int? = null,
        val location: String? = null,
        val priority: String? = null,
        val retries: Int? = null,
        @JsonAlias("key_file")
        val keyfile: String? = null,
        val method: Method? = null,
        val refresh_token: String? = null,
        val client_id: String? = null,
        val client_secret: String? = null,
        val impersonated_credentials: String? = null,
        val token_uri: String? = null,
    ) : Warehouse.Config {
        enum class Method {
            `service-account`, `oauth-secrets`, `service-account-json`, oauth
        }

        override fun toString(): String = "$dataset - $project"
        override fun stripPassword() = this.copy(keyfile_json = "", client_secret = null)
        override fun isValid() = true
        override fun warehouseSchema() = dataset
        override fun warehouseDatabase() = project
    }

    class StringOrObjectDeserializer : JsonDeserializer<String>() {
        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): String? {
            return when (p.currentToken) {
                JsonToken.VALUE_STRING -> p.valueAsString
                JsonToken.START_OBJECT -> JsonHelper.encode(p.readValueAsTree<ObjectNode>())
                else -> throw JsonMappingException(p, "Value must be either an object or string")
            }
        }
    }
}

class BigQueryWarehouseProxy : Warehouse<BigQueryWarehouse.BigQueryConfig> by BigQueryWarehouse
