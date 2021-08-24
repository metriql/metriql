package com.metriql.warehouse.snowflake

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.WarehouseTimeframes

class SnowflakeTimeframes : WarehouseTimeframes {
    override val timestampPostOperations: Map<TimestampPostOperation, String> = mapOf(
        TimestampPostOperation.HOUR to "date_trunc('hour', %s)",
        TimestampPostOperation.DAY to "to_date(date_trunc('day', %s))",
        TimestampPostOperation.WEEK to "to_date(date_trunc('week', %s))",
        TimestampPostOperation.MONTH to "to_date(cast(date_trunc('month', %s) as date))",
        TimestampPostOperation.QUARTER to "to_date(cast(date_trunc('quarter', %s) as date))",
        TimestampPostOperation.YEAR to "to_date(cast(date_trunc('year', %s) as date))",
        TimestampPostOperation.HOUR_OF_DAY to "to_time(lpad(cast(extract(hour FROM %s) as text), 2, '0')||':00')",
        TimestampPostOperation.DAY_OF_MONTH to "EXTRACT(day FROM %s)",
        TimestampPostOperation.WEEK_OF_YEAR to "WEEKOFYEAR(%s)",
        TimestampPostOperation.MONTH_OF_YEAR to """
        CASE monthname(%s) 
            WHEN 'Jan' THEN 'January'
            WHEN 'Feb' THEN 'February'
            WHEN 'Mar' THEN 'March'
            WHEN 'Apr' THEN 'April'
            WHEN 'May' THEN 'May'
            WHEN 'Jun' THEN 'June'
            WHEN 'Jul' THEN 'July'
            WHEN 'Aug' THEN 'August'
            WHEN 'Sep' THEN 'September'
            WHEN 'Oct' THEN 'October'
            WHEN 'Nov' THEN 'November'
            WHEN 'Dec' THEN 'December'
        END
        """.trimIndent(),
        TimestampPostOperation.QUARTER_OF_YEAR to "'Q' || extract(quarter FROM %s)",
        TimestampPostOperation.DAY_OF_WEEK to "decode(extract ('dayofweek_iso', %s), 1, 'Monday', 2, 'Tuesday', 3, 'Wednesday', " +
            "4, 'Thursday', 5, 'Friday', 6, 'Saturday', 7, 'Sunday')"
    )

    override val datePostOperations: Map<DatePostOperation, String> = mapOf(
        DatePostOperation.DAY to "to_date(date_trunc('day', %s))",
        DatePostOperation.WEEK to "DATEADD(day, -1, to_date(cast(date_trunc('week', %s) as date)))",
        DatePostOperation.MONTH to "to_date(cast(date_trunc('month', %s) as date))",
        DatePostOperation.QUARTER to "to_date(cast(date_trunc('quarter', %s) as date))",
        DatePostOperation.YEAR to "to_date(cast(date_trunc('year', %s) as date))",
        DatePostOperation.DAY_OF_MONTH to "EXTRACT(day FROM %s)",
        DatePostOperation.WEEK_OF_YEAR to "WEEKOFYEAR(%s)",
        DatePostOperation.MONTH_OF_YEAR to """
        CASE monthname(%s)
            WHEN 'Jan' THEN 'January'
            WHEN 'Feb' THEN 'February'
            WHEN 'Mar' THEN 'March'
            WHEN 'Apr' THEN 'April'
            WHEN 'May' THEN 'May'
            WHEN 'Jun' THEN 'June'
            WHEN 'Jul' THEN 'July'
            WHEN 'Aug' THEN 'August'
            WHEN 'Sep' THEN 'September'
            WHEN 'Oct' THEN 'October'
            WHEN 'Nov' THEN 'November'
            WHEN 'Dec' THEN 'December'
        END
        """.trimIndent(),
        DatePostOperation.QUARTER_OF_YEAR to "'Q' || extract(quarter FROM %s)",
        DatePostOperation.DAY_OF_WEEK to "dayname(%s)||'day'"
    )

    override val timePostOperations: Map<TimePostOperation, String> = mapOf(
        TimePostOperation.MINUTE to "CAST(extract(hour FROM %1\$s) || ':' || extract(minute FROM %1\$s) || ':00' AS TIME)",
        TimePostOperation.HOUR to "CAST(lpad(cast(extract(hour FROM %s) as text), 2, '0')||':00' AS TIME)"
    )
}
