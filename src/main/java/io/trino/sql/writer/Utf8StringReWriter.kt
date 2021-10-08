package io.trino.sql.writer

import io.trino.operator.scalar.StringFunctions
import io.trino.operator.scalar.VarbinaryFunctions
import io.trino.sql.tree.Expression
import io.trino.sql.tree.ExpressionRewriter
import io.trino.sql.tree.ExpressionTreeRewriter
import io.trino.sql.tree.FunctionCall
import io.trino.sql.tree.StringLiteral

class Utf8StringReWriter : ExpressionRewriter<Void>() {
    // Rewrite utf8 values as StringLiteral to simplify the query
    override fun rewriteFunctionCall(node: FunctionCall, context: Void?, treeRewriter: ExpressionTreeRewriter<Void>?): Expression? {
        if (node.name.suffix.lowercase() == "from_utf8" && node.arguments.size == 1) {
            val innerFunction = node.arguments[0] as? FunctionCall
            if (innerFunction?.name?.suffix?.lowercase() == "from_hex" && node.arguments.size == 1) {
                val innerValue = innerFunction.arguments[0] as? StringLiteral
                if (innerValue != null) {
                    val utf8Value = StringFunctions.fromUtf8(
                        VarbinaryFunctions.fromHexVarbinary(innerValue.slice)
                    ).toStringUtf8()
                    return StringLiteral(utf8Value)
                }
            }
        }

        return super.rewriteFunctionCall(node, context, treeRewriter)
    }
}
