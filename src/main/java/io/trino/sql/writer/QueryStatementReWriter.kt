package io.trino.sql.writer

import io.trino.sql.tree.Expression
import io.trino.sql.tree.ExpressionTreeRewriter
import io.trino.sql.tree.GroupBy
import io.trino.sql.tree.Offset
import io.trino.sql.tree.OrderBy
import io.trino.sql.tree.QuerySpecification
import io.trino.sql.tree.Select
import io.trino.sql.tree.SimpleGroupBy
import io.trino.sql.tree.SingleColumn
import io.trino.sql.tree.SortItem
import io.trino.sql.tree.Unnest
import io.trino.sql.tree.Values
import io.trino.sql.tree.WindowDefinition
import io.trino.sql.tree.WindowSpecification

object QueryStatementReWriter {
    private val rewriters = listOf(
        Utf8StringReWriter()
    )

    fun rewrite(node: QuerySpecification): QuerySpecification {
        return QuerySpecification(
            Select(
                node.select.isDistinct,
                node.select.selectItems.map {
                    when (it) {
                        is SingleColumn -> SingleColumn(rewrite(it.expression), it.alias)
                        else -> it
                    }
                }
            ),
            node.from.map {
                when (it) {
                    is QuerySpecification -> {
                        it
                    }
                    is Unnest -> {
                        it
                    }
                    is Values -> {
                        it
                    }
                    else -> it
                }
            },
            node.where.map { rewrite(it) },
            node.groupBy?.map {
                GroupBy(
                    it.isDistinct,
                    it.groupingElements.map { item ->
                        when (item) {
                            is SimpleGroupBy -> SimpleGroupBy(item.expressions.map { exp -> rewrite(exp) })
                            else -> item
                        }
                    }
                )
            },
            node.having.map { rewrite(it) },
            node.windows.map {
                WindowDefinition(
                    it.name,
                    WindowSpecification(it.window.existingWindowName, it.window.partitionBy.map { exp -> rewrite(exp) }, it.window.orderBy, it.window.frame)
                )
            },
            node.orderBy.map { order -> OrderBy(order.sortItems.map { SortItem(rewrite(it.sortKey), it.ordering, it.nullOrdering) }) },
            node.offset.map { Offset(rewrite(it.rowCount)) },
            node.limit
        )
    }

    private fun rewrite(expression: Expression): Expression {
        var exp = expression
        for (rewriter in rewriters) {
            exp = ExpressionTreeRewriter.rewriteWith(rewriter, exp)
        }
        return exp
    }
}
