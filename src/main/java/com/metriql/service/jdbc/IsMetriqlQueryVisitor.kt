package com.metriql.service.jdbc

import io.trino.sql.tree.DefaultTraversalVisitor
import io.trino.sql.tree.Relation
import io.trino.sql.tree.Table
import java.util.concurrent.atomic.AtomicReference

const val PROPERTIES_TABLE_SUFFIX = "\$properties"

fun extractModelNameFromPropertiesTable(tableName: String): String? {
    return if (tableName.endsWith(PROPERTIES_TABLE_SUFFIX) && tableName.length > PROPERTIES_TABLE_SUFFIX.length) {
        tableName.substring(0, tableName.length - PROPERTIES_TABLE_SUFFIX.length)
    } else null
}

class IsMetriqlQueryVisitor(private val defaultCatalog: String) : DefaultTraversalVisitor<AtomicReference<Boolean?>>() {
    override fun visitRelation(node: Relation, context: AtomicReference<Boolean?>): Void? {
        when (node) {
            is Table -> {
                val catalog = if (node.name.parts.size == 3) {
                    node.name.parts[0].lowercase()
                } else {
                    defaultCatalog
                }

                if (catalog == "metriql") {
                    val schema = node.name.prefix.orElse(null)?.suffix
                    val isMetadata = schema == "information_schema" || extractModelNameFromPropertiesTable(node.name.suffix) != null

                    if (!isMetadata) {
                        context.set(true)
                    }
                } else if (catalog == "system") {
                    context.set(false)
                }
            }
        }

        return null
    }
}
