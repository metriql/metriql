package com.metriql.warehouse.clickhouse

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.WarehouseTimeframes

// CAST(val AS DATE) throws an exception is val is NULL
class ClickhouseTimeframes : WarehouseTimeframes {
    override val timestampPostOperations: Map<TimestampPostOperation, String> = mapOf(
        TimestampPostOperation.HOUR to "CAST(DATE_TRUNC('hour', %s), 'Nullable(datetime)')",
        TimestampPostOperation.DAY to "CAST(DATE_TRUNC('day', %s), 'Nullable(date)')",
        TimestampPostOperation.WEEK to "CAST(DATE_TRUNC('week', %s), 'Nullable(date)')",
        TimestampPostOperation.MONTH to "CAST(DATE_TRUNC('month', %s), 'Nullable(date)')",
        TimestampPostOperation.QUARTER to "CAST(DATE_TRUNC('quarter', %s), 'Nullable(date)')",
        TimestampPostOperation.YEAR to "CAST(DATE_TRUNC('year', %s), 'Nullable(date)')",
        TimestampPostOperation.HOUR_OF_DAY to "formatDateTime(%s, '%%H:%%M')",
        TimestampPostOperation.DAY_OF_MONTH to "toDayOfMonth(%s)",
        TimestampPostOperation.WEEK_OF_YEAR to "toWeek(%s, 3)",
        TimestampPostOperation.MONTH_OF_YEAR to "toMonth(%s)",
        TimestampPostOperation.QUARTER_OF_YEAR to "CONCAT('Q', toString(toQuarter(%s)))",
        TimestampPostOperation.DAY_OF_WEEK to "toDayOfWeek(%s)"
    )

    override val datePostOperations: Map<DatePostOperation, String> = mapOf(
        DatePostOperation.DAY to "CAST(%s, 'Nullable(date)')",
        DatePostOperation.WEEK to "CAST(DATE_TRUNC('week', %s), 'Nullable(date)')",
        DatePostOperation.MONTH to "CAST(DATE_TRUNC('month', %s), 'Nullable(date)')",
        DatePostOperation.QUARTER to "CAST(DATE_TRUNC('quarter', %s), 'Nullable(date)')",
        DatePostOperation.YEAR to "CAST(DATE_TRUNC('year', %s), 'Nullable(date)')",
        DatePostOperation.DAY_OF_MONTH to "EXTRACT(day FROM %s)",
        DatePostOperation.WEEK_OF_YEAR to "toISOWeek(%s)",
        DatePostOperation.MONTH_OF_YEAR to "toMonth(%s)",
        DatePostOperation.QUARTER_OF_YEAR to "CONCAT('Q', toString(toQuarter(%s)))",
        DatePostOperation.DAY_OF_WEEK to "toDayOfWeek(%s)"
    )

    override val timePostOperations: Map<TimePostOperation, String> = mapOf()
}
