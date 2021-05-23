package com.metriql.warehouse.mysql

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.WarehouseTimeframes

class MySQLTimeframes : WarehouseTimeframes {
    override val timestampPostOperations: Map<TimestampPostOperation, String> = mapOf(
        TimestampPostOperation.HOUR to "STR_TO_DATE(DATE_FORMAT(%s, '%%Y-%%m-%%d %%H:00:00'), '%%Y-%%m-%%d %%T')",
        TimestampPostOperation.DAY to "STR_TO_DATE(DATE_FORMAT(%s, '%%Y-%%m-%%d'), '%%Y-%%m-%%d')",
        TimestampPostOperation.WEEK to "CAST(DATE_SUB(%s, interval DAYOFWEEK(%<s) - 1 day) AS DATE)",
        TimestampPostOperation.MONTH to "STR_TO_DATE(DATE_FORMAT(%s, '%%Y-%%m-01'), '%%Y-%%m-%%d')",
        TimestampPostOperation.YEAR to "STR_TO_DATE(DATE_FORMAT(%s, '%%Y-%%01-01'), '%%Y-%%m-%%d')",
        TimestampPostOperation.HOUR_OF_DAY to "TIME(DATE_FORMAT(%s, '%%H:00'))",
        TimestampPostOperation.DAY_OF_MONTH to "DAYOFMONTH(%s)",
        TimestampPostOperation.WEEK_OF_YEAR to "WEEKOFYEAR(%s)",
        TimestampPostOperation.MONTH_OF_YEAR to "MONTHNAME(%s)",
        TimestampPostOperation.QUARTER_OF_YEAR to "CONCAT('Q', QUARTER(%s))",
        TimestampPostOperation.DAY_OF_WEEK to "DAYNAME(%s)"
    )
    override val timePostOperations: Map<TimePostOperation, String> = mapOf(
        TimePostOperation.MINUTE to "CAST(CONCAT(CONCAT(CONCAT(extract(hour FROM %s), ':'), extract(minute FROM %<s)), ':00') AS TIME)",
        TimePostOperation.HOUR to "CAST(CONCAT(extract(hour FROM %s), ':00:00') AS TIME)"
    )
    override val datePostOperations: Map<DatePostOperation, String> = mapOf(
        DatePostOperation.DAY to "CAST(%s AS DATE)",
        DatePostOperation.WEEK to "DATE_SUB(%s, interval DAYOFWEEK(%<s) - 1 day)",
        DatePostOperation.MONTH to "STR_TO_DATE(DATE_FORMAT(%s, '%%Y-%%m-01'), '%%Y-%%m-%%d')",
        DatePostOperation.YEAR to "STR_TO_DATE(DATE_FORMAT(%s, '%%Y-%%01-01'), '%%Y-%%m-%%d')",
        DatePostOperation.DAY_OF_MONTH to "DAYOFMONTH(%s)",
        DatePostOperation.WEEK_OF_YEAR to "WEEKOFYEAR(%s)",
        DatePostOperation.MONTH_OF_YEAR to "MONTHNAME(%s)",
        DatePostOperation.QUARTER_OF_YEAR to "CONCAT('Q', QUARTER(%s))",
        DatePostOperation.DAY_OF_WEEK to "DAYNAME(%s)"
    )
}
