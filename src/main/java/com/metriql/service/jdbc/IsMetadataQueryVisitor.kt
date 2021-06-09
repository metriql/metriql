package com.metriql.service.jdbc

import io.trino.sql.tree.DefaultTraversalVisitor
import io.trino.sql.tree.Relation
import io.trino.sql.tree.Table
import java.util.concurrent.atomic.AtomicReference

class IsMetadataQueryVisitor : DefaultTraversalVisitor<AtomicReference<Boolean?>>() {
    override fun visitRelation(node: Relation, context: AtomicReference<Boolean?>): Void? {
        when (node) {
            is Table -> {
                val catalog = if (node.name.parts.size == 3) {
                    node.name.parts[0].lowercase()
                } else {
                    "metriql"
                }

                if (catalog == "system") {
                    context.set(true)
                } else if (catalog != "metriql") {
                    throw UnsupportedOperationException("$catalog catalog is not available. You `metriql` or `system`")
                }
            }
            else -> throw UnsupportedOperationException("${node.javaClass.name} operation is not supported.")
        }

        return null
    }
}
