package com.metriql.warehouse.clickhouse

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.WarehouseTimeframes

class ClickhouseTimeframes : WarehouseTimeframes {
    override val timestampPostOperations: Map<TimestampPostOperation, String> = mapOf(
        TimestampPostOperation.HOUR to "CAST(DATE_TRUNC('hour', %s) AS TIMESTAMP)",
        TimestampPostOperation.DAY to "CAST(DATE_TRUNC('day', %s) AS DATE)",
        TimestampPostOperation.WEEK to "CAST(DATE_TRUNC('week', %s) AS DATE)",
        TimestampPostOperation.MONTH to "CAST(DATE_TRUNC('month', %s) AS DATE)",
        TimestampPostOperation.YEAR to "CAST(DATE_TRUNC('year', %s) AS DATE)",
        TimestampPostOperation.HOUR_OF_DAY to "formatDateTime(%s, '%H:%M')",
        TimestampPostOperation.DAY_OF_MONTH to "toDayOfMonth(%s)",
        TimestampPostOperation.WEEK_OF_YEAR to "toWeek(%s, 3)",
        TimestampPostOperation.MONTH_OF_YEAR to "dateName('month', %s)",
        TimestampPostOperation.QUARTER_OF_YEAR to "'Q' ||dateName('quarter', %s)",
        TimestampPostOperation.DAY_OF_WEEK to "toDayOfWeek(%s)"
    )

    override val datePostOperations: Map<DatePostOperation, String> = mapOf(
        DatePostOperation.DAY to "CAST(%s AS DATE)",
        DatePostOperation.WEEK to "CAST(DATE_TRUNC('week', %s) AS DATE)",
        DatePostOperation.MONTH to "CAST(DATE_TRUNC('month', %s) AS DATE)",
        DatePostOperation.YEAR to "CAST(DATE_TRUNC('year', %s) AS DATE)",
        DatePostOperation.DAY_OF_MONTH to "EXTRACT(day FROM %s)",
        DatePostOperation.WEEK_OF_YEAR to "EXTRACT(week FROM %s)",
        DatePostOperation.MONTH_OF_YEAR to "dateName('month', %s)",
        DatePostOperation.QUARTER_OF_YEAR to "'Q' ||dateName('quarter', %s)",
        DatePostOperation.DAY_OF_WEEK to "toDayOfWeek(%s)"
    )

    override val timePostOperations: Map<TimePostOperation, String> = mapOf()
}
