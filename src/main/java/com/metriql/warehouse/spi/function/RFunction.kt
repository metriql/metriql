package com.metriql.warehouse.spi.function

import com.metriql.db.FieldType
import com.metriql.db.FieldType.DATE
import com.metriql.db.FieldType.INTEGER
import com.metriql.db.FieldType.STRING
import com.metriql.db.FieldType.TIMESTAMP
import com.metriql.util.UppercaseEnum

// for documentation
typealias AcceptedTypes = List<FieldType>
typealias Parameter = Pair<String, AcceptedTypes>

@UppercaseEnum
enum class RFunction(val description: String?, val returnType: FieldType, val parameters: List<Parameter>) {
    NOW("Returns the current timestamp", TIMESTAMP, listOf()),

    DATE_DIFF(
        "Returns the difference of two timestamp given the period", INTEGER,
        listOf(
            "from" to listOf(TIMESTAMP, DATE, TIMESTAMP),
            "to" to listOf(TIMESTAMP),
            /* Date Period (day, week, month, etc.)  */
            "period" to listOf(STRING)
        )
    ),

    DATE_ADD(
        "Adds the given period to the timestamp", TIMESTAMP,
        listOf(
            "timestamp" to listOf(TIMESTAMP),
            /* Date Period (day, week, month, etc.)  */
            "period" to listOf(STRING),
            /* Number of periods to add  */
            "value" to listOf(INTEGER)
        )
    ),

    HEX_TO_INT(
        "Generates a hash of a HEX value", FieldType.LONG,
        listOf(
            "hex" to listOf(STRING)
        )
    ),

    SUBSTRING(
        "Returns a subset of a string", STRING,
        listOf(
            "value" to listOf(STRING),
            "start" to listOf(INTEGER),
            "end" to listOf(INTEGER)
        )
    ),

    TO_ISO8601(
        "Format as an ISO 8601 string. The parameter can be date or timestamp",
        STRING,
        listOf(
            "timestamp" to listOf(TIMESTAMP)
        )
    ),

    CEIL(
        null,
        INTEGER,
        listOf(
            "number" to listOf(FieldType.DOUBLE)
        )
    ),

    FLOOR(
        null, INTEGER,
        listOf(
            "number" to listOf(FieldType.DOUBLE)
        )
    ),

    ROUND(
        null, INTEGER,
        listOf(
            "number" to listOf(FieldType.DOUBLE)
        )
    )
}
