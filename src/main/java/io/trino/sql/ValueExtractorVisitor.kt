package io.trino.sql

import com.metriql.util.MetriqlException
import io.netty.handler.codec.http.HttpResponseStatus
import io.trino.sql.tree.AstVisitor
import io.trino.sql.tree.BooleanLiteral
import io.trino.sql.tree.Cast
import io.trino.sql.tree.CharLiteral
import io.trino.sql.tree.DateTimeDataType
import io.trino.sql.tree.DecimalLiteral
import io.trino.sql.tree.DoubleLiteral
import io.trino.sql.tree.GenericDataType
import io.trino.sql.tree.GenericLiteral
import io.trino.sql.tree.IntervalLiteral
import io.trino.sql.tree.LongLiteral
import io.trino.sql.tree.Node
import io.trino.sql.tree.NullLiteral
import io.trino.sql.tree.StringLiteral
import io.trino.sql.tree.TimeLiteral
import io.trino.sql.tree.TimestampLiteral
import java.time.Instant

class ValueExtractorVisitor : AstVisitor<Any, Void>() {
    override fun visitNode(node: Node?, context: Void?): Any {
        throw MetriqlException("Unable to parse $node", HttpResponseStatus.BAD_REQUEST)
    }

    override fun visitLongLiteral(node: LongLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitDoubleLiteral(node: DoubleLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitDecimalLiteral(node: DecimalLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitGenericLiteral(node: GenericLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitTimeLiteral(node: TimeLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitTimestampLiteral(node: TimestampLiteral, context: Void?): Any {
        return Instant.parse(node.value)
    }

    override fun visitIntervalLiteral(node: IntervalLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitStringLiteral(node: StringLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitCharLiteral(node: CharLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitBooleanLiteral(node: BooleanLiteral, context: Void?): Any {
        return node.value
    }

    override fun visitNullLiteral(node: NullLiteral?, context: Void?): Any {
        return "NULL"
    }

    override fun visitCast(node: Cast, context: Void?): Any {
        val currentVal = this.process(node.expression)
        return when (val x = node.type) {
            is GenericDataType -> currentVal
            is DateTimeDataType -> throw MetriqlException("Unable to support $node", HttpResponseStatus.BAD_REQUEST)
            else -> throw MetriqlException("Unable to support $node", HttpResponseStatus.BAD_REQUEST)
        }
    }
}
