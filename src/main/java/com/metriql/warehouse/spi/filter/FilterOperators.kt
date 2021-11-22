package com.metriql.warehouse.spi.filter

import com.metriql.util.UppercaseEnum
import java.time.LocalDate

data class DateRange(val start: LocalDate, val end: LocalDate)

interface FilterOperator

@UppercaseEnum
enum class AnyOperatorType : FilterOperator {
    IS_SET,
    IS_NOT_SET;
}


@UppercaseEnum
enum class StringOperatorType : FilterOperator {
    EQUALS,
    NOT_EQUALS,
    IN,
    NOT_IN,
    REGEX,
    CONTAINS,
    STARTS_WITH,
    ENDS_WITH,
    NOT_CONTAINS;
}

@UppercaseEnum
enum class NumberOperatorType : FilterOperator {
    NOT_EQUALS,
    LESS_THAN,
    EQUALS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN_OR_EQUAL;
}

@UppercaseEnum
enum class DateOperatorType : FilterOperator {
    EQUALS,
    LESS_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    BETWEEN;
}

@UppercaseEnum
enum class ArrayOperatorType : FilterOperator {
    INCLUDES,
    NOT_INCLUDES
}

@UppercaseEnum
enum class TimeOperatorType : FilterOperator {
    EQUALS,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN_OR_EQUAL,
    LESS_THAN,
    GREATER_THAN;
}

@UppercaseEnum
enum class TimestampOperatorType : FilterOperator {
    EQUALS,
    LESS_THAN,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN_OR_EQUAL,
    BETWEEN;
}

@UppercaseEnum
enum class BooleanOperatorType : FilterOperator {
    IS;
}
