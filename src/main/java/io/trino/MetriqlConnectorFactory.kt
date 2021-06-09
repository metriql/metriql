package io.trino

import com.fasterxml.jackson.annotation.JsonProperty
import com.metriql.service.model.Model
import io.trino.connector.system.SystemHandleResolver
import io.trino.connector.system.SystemTablesMetadata
import io.trino.spi.connector.Connector
import io.trino.spi.connector.ConnectorContext
import io.trino.spi.connector.ConnectorFactory
import io.trino.spi.connector.ConnectorHandleResolver
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.transaction.IsolationLevel
import io.trino.transaction.InternalConnector
import io.trino.transaction.TransactionId

class MetriqlConnectorFactory(val models: List<Model>) : ConnectorFactory {
    override fun getName() = "metriql"

    override fun create(catalogName: String?, config: MutableMap<String, String>?, context: ConnectorContext?): Connector {
        return MetriqlConnector(models)
    }

    override fun getHandleResolver(): ConnectorHandleResolver {
        return MetriqlHandleResolver()
    }

    class MetriqlConnector(val models: List<Model>) : InternalConnector {
        override fun beginTransaction(transactionId: TransactionId, isolationLevel: IsolationLevel?, readOnly: Boolean): ConnectorTransactionHandle {
            return MetriqlTransactionHandle(transactionId)
        }

        override fun beginTransaction(isolationLevel: IsolationLevel?, readOnly: Boolean): ConnectorTransactionHandle {
            return super.beginTransaction(isolationLevel, readOnly)
        }

        override fun getMetadata(transactionHandle: ConnectorTransactionHandle?) = SystemTablesMetadata(MetriqlMetadata(models))
    }

    data class MetriqlTransactionHandle(@JsonProperty("transactionId") val transactionId: TransactionId) : ConnectorTransactionHandle

    class MetriqlHandleResolver : SystemHandleResolver() {
        override fun getTransactionHandleClass(): Class<out ConnectorTransactionHandle?> {
            return MetriqlTransactionHandle::class.java
        }
    }
}
