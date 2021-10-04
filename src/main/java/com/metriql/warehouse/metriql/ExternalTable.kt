package com.metriql.warehouse.metriql

import com.metriql.warehouse.metriql.storage.ObjectStore
import io.trino.decoder.RowDecoder
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTableMetadata
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.connector.SystemTable
import io.trino.spi.predicate.TupleDomain

class ExternalTable(val session: ConnectorSession, val client: ObjectStore, private val rowDecoder: RowDecoder, val path: String) : SystemTable {
    override fun getDistribution(): SystemTable.Distribution {
        TODO("not implemented")
    }

    override fun getTableMetadata(): ConnectorTableMetadata {
        TODO("not implemented")
    }

    override fun pageSource(transactionHandle: ConnectorTransactionHandle, session: ConnectorSession?, constraint: TupleDomain<Int?>?): ConnectorPageSource {
        val stream = client.get(path)
        if (stream != null) {

        }
        val decodeRow = rowDecoder.decodeRow(null)
        return null!!
    }
}
