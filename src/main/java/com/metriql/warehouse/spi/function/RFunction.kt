package com.metriql.warehouse.spi.function

import com.metriql.db.FieldType
import com.metriql.util.UppercaseEnum

typealias Placeholder = String
typealias Parameter = Pair<Placeholder, FieldType>

@UppercaseEnum
enum class RFunction(val description: String, val returnType: FieldType, val parameters: List<Parameter>) {
    NOW("Returns the current timestamp", FieldType.TIMESTAMP, listOf()),

    DATE_DIFF(
        "Returns the difference of two timestamp given the period", FieldType.INTEGER,
        listOf(
            "from" to FieldType.TIMESTAMP,
            "to" to FieldType.TIMESTAMP,
            /* Date Period (day, week, month, etc.)  */
            "period" to FieldType.STRING
        )
    ),

    DATE_ADD(
        "Adds the given period to the timestamp", FieldType.TIMESTAMP,
        listOf(
            "timestamp" to FieldType.TIMESTAMP,
            /* Date Period (day, week, month, etc.)  */
            "period" to FieldType.STRING,
            /* Number of periods to add  */
            "value" to FieldType.INTEGER
        )
    ),

    HEX_TO_INT(
        "Generates a hash of a HEX value", FieldType.LONG,
        listOf(
            "hex" to FieldType.STRING
        )
    ),
}
