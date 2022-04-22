package io.trino.execution

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import io.trino.execution.warnings.WarningCollector
import io.trino.metadata.Metadata
import io.trino.security.AccessControl
import io.trino.sql.tree.Expression
import io.trino.sql.tree.StartTransaction
import io.trino.transaction.TransactionManager

class NoOpStartTransactionTask : StartTransactionTask() {
    override fun execute(
        statement: StartTransaction?,
        transactionManager: TransactionManager?,
        metadata: Metadata?,
        accessControl: AccessControl?,
        stateMachine: QueryStateMachine?,
        parameters: MutableList<Expression>?,
        warningCollector: WarningCollector?
    ): ListenableFuture<*> {
        return Futures.immediateFuture<Any>(null)
    }
}
