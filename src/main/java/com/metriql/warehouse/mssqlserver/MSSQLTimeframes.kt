package com.metriql.warehouse.mssqlserver

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.WarehouseTimeframes

class MSSQLTimeframes : WarehouseTimeframes {
    override val timestampPostOperations: Map<TimestampPostOperation, String> = mapOf(
        TimestampPostOperation.HOUR to "DATEADD(hour, DATEDIFF(hour, 0, %s), 0)",
        TimestampPostOperation.DAY to "DATEADD(day, DATEDIFF(day, 0, %s), 0)",
        TimestampPostOperation.WEEK to "DATEADD(week, DATEDIFF(week, 0, %s), 0)",
        TimestampPostOperation.MONTH to "DATEADD(month, DATEDIFF(month, 0, %s), 0)",
        TimestampPostOperation.YEAR to "DATEADD(year, DATEDIFF(year, 0, %s), 0)",
        TimestampPostOperation.HOUR_OF_DAY to "CAST(FORMAT(%s, 'hh:00') AS TIME)",
        TimestampPostOperation.DAY_OF_MONTH to "DATEPART(dd, %s)",
        TimestampPostOperation.WEEK_OF_YEAR to "DATEPART(ww, %s)",
        TimestampPostOperation.MONTH_OF_YEAR to "DATENAME(month, %s)",
        TimestampPostOperation.QUARTER_OF_YEAR to "CONCAT('Q', DATEPART(qq, %s))",
        TimestampPostOperation.DAY_OF_WEEK to "FORMAT(%s, 'dddd')"
    )
    override val timePostOperations: Map<TimePostOperation, String> = mapOf(
        TimePostOperation.MINUTE to "CAST(CONCAT('00:', datepart(minute, %s), ':00') AS TIME)",
        TimePostOperation.HOUR to "CAST(CONCAT(datepart(hour, %s), ':00:00') AS TIME)"
    )
    override val datePostOperations: Map<DatePostOperation, String> = mapOf(
        DatePostOperation.DAY to "CAST(%s AS DATE)",
        DatePostOperation.WEEK to "DATEADD(week, DATEDIFF(week, 0, %s), 0)",
        DatePostOperation.MONTH to "DATEADD(month, DATEDIFF(month, 0, %s), 0)",
        DatePostOperation.YEAR to "DATEADD(year, DATEDIFF(year, 0, %s), 0)",
        DatePostOperation.DAY_OF_MONTH to "DATEPART(dd, %s)",
        DatePostOperation.WEEK_OF_YEAR to "DATEPART(ww, %s)",
        DatePostOperation.MONTH_OF_YEAR to "DATENAME(month, %s)",
        DatePostOperation.QUARTER_OF_YEAR to "CONCAT('Q', DATEPART(qq, %s))",
        DatePostOperation.DAY_OF_WEEK to "FORMAT(%s, 'dddd')"
    )
}
