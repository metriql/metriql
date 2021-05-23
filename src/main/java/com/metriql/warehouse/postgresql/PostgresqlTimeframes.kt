package com.metriql.warehouse.postgresql

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.WarehouseTimeframes

open class PostgresqlTimeframes : WarehouseTimeframes {
    override val timestampPostOperations: Map<TimestampPostOperation, String> = mapOf(
        TimestampPostOperation.HOUR to "CAST(DATE_TRUNC('hour', %s) AS TIMESTAMP)",
        TimestampPostOperation.DAY to "CAST(DATE_TRUNC('day', %s) AS DATE)",
        TimestampPostOperation.WEEK to "CAST(DATE_TRUNC('week', %s) AS DATE)",
        TimestampPostOperation.MONTH to "CAST(DATE_TRUNC('month', %s) AS DATE)",
        TimestampPostOperation.YEAR to "CAST(DATE_TRUNC('year', %s) AS DATE)",
        TimestampPostOperation.HOUR_OF_DAY to "CAST(lpad(cast(extract(hour FROM %s) as text), 2, '0')||':00' AS TIME)",
        TimestampPostOperation.DAY_OF_MONTH to "EXTRACT(day FROM %s)",
        TimestampPostOperation.WEEK_OF_YEAR to "EXTRACT(week FROM %s)",
        TimestampPostOperation.MONTH_OF_YEAR to "rtrim(to_char(%s, 'Month'))",
        TimestampPostOperation.QUARTER_OF_YEAR to "'Q' || EXTRACT(quarter FROM %s)",
        TimestampPostOperation.DAY_OF_WEEK to "rtrim(to_char(%s, 'Day'))"
    )

    override val datePostOperations: Map<DatePostOperation, String> = mapOf(
        DatePostOperation.DAY to "CAST(%s AS DATE)",
        DatePostOperation.WEEK to "CAST(DATE_TRUNC('week', %s) AS DATE)",
        DatePostOperation.MONTH to "CAST(DATE_TRUNC('month', %s) AS DATE)",
        DatePostOperation.YEAR to "CAST(DATE_TRUNC('year', %s) AS DATE)",
        DatePostOperation.DAY_OF_MONTH to "EXTRACT(day FROM %s)",
        DatePostOperation.WEEK_OF_YEAR to "EXTRACT(week FROM %s)",
        DatePostOperation.MONTH_OF_YEAR to "rtrim(to_char(%s, 'Month'))",
        DatePostOperation.QUARTER_OF_YEAR to "'Q' || EXTRACT(quarter FROM %s)",
        DatePostOperation.DAY_OF_WEEK to "rtrim(to_char(%s, 'Day'))"
    )

    override val timePostOperations: Map<TimePostOperation, String> = mapOf(
        TimePostOperation.MINUTE to "CAST(extract(hour FROM %s) || ':' || extract(minute FROM %<s) || ':00' AS TIME)",
        TimePostOperation.HOUR to "CAST(lpad(cast(extract(hour FROM %s) as text), 2, '0')||':00' AS TIME)"
    )
}
