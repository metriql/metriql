package io.trino

import com.metriql.service.dataset.IDatasetService
import io.trino.connector.system.SystemHandleResolver
import io.trino.connector.system.SystemPageSourceProvider
import io.trino.connector.system.SystemSplitManager
import io.trino.connector.system.SystemTablesMetadata
import io.trino.connector.system.SystemTransactionHandle
import io.trino.metadata.InternalNodeManager
import io.trino.spi.connector.Connector
import io.trino.spi.connector.ConnectorContext
import io.trino.spi.connector.ConnectorFactory
import io.trino.spi.connector.ConnectorHandleResolver
import io.trino.spi.connector.ConnectorPageSourceProvider
import io.trino.spi.connector.ConnectorSplitManager
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.session.PropertyMetadata
import io.trino.spi.transaction.IsolationLevel
import io.trino.spi.type.MapType
import io.trino.transaction.InternalConnector
import io.trino.transaction.TransactionId
import io.trino.type.MapParametricType

class MetriqlConnectorFactory(private val internalNodeManager: InternalNodeManager, val datasetService: IDatasetService) : ConnectorFactory {
    override fun getName() = "metriql"

    override fun create(catalogName: String?, config: MutableMap<String, String>?, context: ConnectorContext?): Connector {
        return MetriqlConnector(internalNodeManager, datasetService)
    }

    override fun getHandleResolver(): ConnectorHandleResolver {
        return SystemHandleResolver()
    }

    class MetriqlConnector(private val nodeManager: InternalNodeManager, val datasetService: IDatasetService) : InternalConnector {
        val metadata = MetriqlMetadata(datasetService)

        override fun beginTransaction(transactionId: TransactionId, isolationLevel: IsolationLevel?, readOnly: Boolean): ConnectorTransactionHandle {
            return MetriqlTransactionHandle(transactionId)
        }

        override fun getSplitManager(): ConnectorSplitManager {
            return SystemSplitManager(nodeManager, metadata)
        }

        override fun getPageSourceProvider(): ConnectorPageSourceProvider {
            return SystemPageSourceProvider(metadata)
        }

        override fun getMetadata(transactionHandle: ConnectorTransactionHandle?) = SystemTablesMetadata(metadata)
    }

    class MetriqlTransactionHandle(transactionId: TransactionId?) : SystemTransactionHandle(transactionId, object : ConnectorTransactionHandle {})

    companion object {
        val QUERY_TYPE_PROPERTY: PropertyMetadata<String> = PropertyMetadata.stringProperty("query_mode", "Switch query mode", "mql", false)
        val METRIQL_AUTH_PROPERTY: PropertyMetadata<String> = PropertyMetadata.stringProperty("metriql", "Metriql info", null, true)
    }
}
