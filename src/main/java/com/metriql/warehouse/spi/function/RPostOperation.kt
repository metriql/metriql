package com.metriql.warehouse.spi.function

import com.metriql.db.FieldType
import com.metriql.util.UppercaseEnum

interface IPostOperation {
    val category: String
    val valueType: FieldType
    val isInclusive: (IPostOperation) -> Boolean
}

@UppercaseEnum
enum class DatePostOperation(
    override val category: String,
    override val valueType: FieldType,
    override val isInclusive: (IPostOperation) -> Boolean = { false }
) : IPostOperation {
    DAY(
        "Date period",
        FieldType.DATE,
        { it is DatePostOperation && it.ordinal > DAY.ordinal }
    ),
    WEEK(
        "Date period",
        FieldType.DATE,
        { it is DatePostOperation && it.ordinal > WEEK.ordinal }
    ),
    MONTH(
        "Date period",
        FieldType.DATE,
        { it is DatePostOperation && it.ordinal > MONTH.ordinal }
    ),
    YEAR(
        "Date period",
        FieldType.DATE,
        { it is DatePostOperation && it.ordinal > YEAR.ordinal }
    ),

    DAY_OF_WEEK("Date category", FieldType.STRING),
    DAY_OF_MONTH("Date category", FieldType.INTEGER),
    WEEK_OF_YEAR("Date category", FieldType.INTEGER),
    MONTH_OF_YEAR("Date category", FieldType.STRING),
    QUARTER_OF_YEAR("Date category", FieldType.STRING),
}

@UppercaseEnum
enum class TimePostOperation(
    override val category: String,
    override val valueType: FieldType,
    override val isInclusive: (IPostOperation) -> Boolean = { false }
) : IPostOperation {
    MINUTE(
        "Date period",
        FieldType.TIME,
        { it == HOUR }
    ),
    HOUR(
        "Date period",
        FieldType.TIME
    );
}

@UppercaseEnum
enum class TimestampPostOperation(
    override val category: String,
    override val valueType: FieldType,
    override val isInclusive: (IPostOperation) -> Boolean = { false }
) : IPostOperation {
    HOUR(
        "Date period",
        FieldType.TIMESTAMP,
        { it is TimestampPostOperation && it.ordinal > HOUR.ordinal }
    ),
    DAY(
        "Date period",
        FieldType.DATE,
        { it is TimestampPostOperation && it.ordinal > DAY.ordinal }
    ),
    WEEK(
        "Date period",
        FieldType.DATE,
        { it is TimestampPostOperation && it.ordinal > WEEK.ordinal }
    ),
    MONTH(
        "Date period",
        FieldType.DATE,
        { it is TimestampPostOperation && it.ordinal > MONTH.ordinal }
    ),
    YEAR(
        "Date period",
        FieldType.DATE,
        { it is TimestampPostOperation && it.ordinal > YEAR.ordinal }
    ),

    WEEK_OF_YEAR("Date category", FieldType.INTEGER, { it == MONTH_OF_YEAR }),
    MONTH_OF_YEAR("Date category", FieldType.STRING, { false }),

    HOUR_OF_DAY("Date category", FieldType.TIME),
    DAY_OF_WEEK("Date category", FieldType.STRING),
    DAY_OF_MONTH("Date category", FieldType.INTEGER),
    QUARTER_OF_YEAR("Date category", FieldType.STRING);
}
