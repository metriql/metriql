package com.metriql.warehouse.bigquery

import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.function.WarehousePostOperation
import com.metriql.warehouse.spi.function.WarehouseTimeframes

class BigQueryTimeframes : WarehouseTimeframes {
    override val timePostOperations: WarehousePostOperation<TimePostOperation> = mapOf(
        TimePostOperation.HOUR to "TIME_TRUNC(%s, HOUR)",
        TimePostOperation.MINUTE to "TIME_TRUNC(%s, MINUTE)"
    )

    override val datePostOperations: Map<DatePostOperation, String> = mapOf(
        DatePostOperation.DAY to "DATE_TRUNC(%s, DAY)",
        DatePostOperation.WEEK to "DATE_TRUNC(%s, WEEK)",
        DatePostOperation.MONTH to "DATE_TRUNC(%s, MONTH)",
        DatePostOperation.YEAR to "DATE_TRUNC(%s, YEAR)",
        DatePostOperation.DAY_OF_WEEK to """
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM %s) = 2 THEN 'Monday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 3 THEN 'Tuesday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 4 THEN 'Wednesday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 5 THEN 'Thursday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 6 THEN 'Friday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 7 THEN 'Saturday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 1 THEN 'Sunday'
        END
        """.trimIndent(),
        DatePostOperation.DAY_OF_MONTH to "EXTRACT(DAY FROM %s)",
        // it's 0 indexed
        DatePostOperation.WEEK_OF_YEAR to "EXTRACT(WEEK FROM %s) + 1",
        DatePostOperation.MONTH_OF_YEAR to """
        CASE
            WHEN EXTRACT(MONTH FROM %s) = 1 THEN 'January'
            WHEN EXTRACT(MONTH FROM %<s) = 2 THEN 'February'
            WHEN EXTRACT(MONTH FROM %<s) = 3 THEN 'March'
            WHEN EXTRACT(MONTH FROM %<s) = 4 THEN 'April'
            WHEN EXTRACT(MONTH FROM %<s) = 5 THEN 'May'
            WHEN EXTRACT(MONTH FROM %<s) = 6 THEN 'June'
            WHEN EXTRACT(MONTH FROM %<s) = 7 THEN 'July'
            WHEN EXTRACT(MONTH FROM %<s) = 8 THEN 'August'
            WHEN EXTRACT(MONTH FROM %<s) = 9 THEN 'September'
            WHEN EXTRACT(MONTH FROM %<s) = 10 THEN 'October'
            WHEN EXTRACT(MONTH FROM %<s) = 11 THEN 'November'
            WHEN EXTRACT(MONTH FROM %<s) = 12 THEN 'December'
        END
        """.trimIndent(),
        DatePostOperation.QUARTER_OF_YEAR to "CONCAT('Q', CAST(EXTRACT(QUARTER FROM %s) AS STRING))"

    )

    override val timestampPostOperations: Map<TimestampPostOperation, String> = mapOf(
        TimestampPostOperation.HOUR to "TIMESTAMP_TRUNC(%s, HOUR)",
        TimestampPostOperation.DAY to "CAST(TIMESTAMP_TRUNC(%s, DAY) AS DATE)",
        TimestampPostOperation.WEEK to "CAST(TIMESTAMP_TRUNC(%s, ISOWEEK) AS DATE)",
        TimestampPostOperation.MONTH to "CAST(TIMESTAMP_TRUNC(%s, MONTH) AS DATE)",
        TimestampPostOperation.YEAR to "CAST(TIMESTAMP_TRUNC(%s, YEAR) AS DATE)",
        TimestampPostOperation.HOUR_OF_DAY to "CAST(CONCAT(LPAD(CAST(EXTRACT(HOUR FROM %s) AS STRING), 2, '0'), ':00:00') AS TIME)",
        TimestampPostOperation.DAY_OF_WEEK to """
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM %s) = 2 THEN 'Monday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 3 THEN 'Tuesday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 4 THEN 'Wednesday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 5 THEN 'Thursday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 6 THEN 'Friday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 7 THEN 'Saturday'
            WHEN EXTRACT(DAYOFWEEK FROM %<s) = 1 THEN 'Sunday'
        END
        """.trimIndent(),
        TimestampPostOperation.DAY_OF_MONTH to "EXTRACT(DAY FROM %s)",
        // it's 0 indexed
        TimestampPostOperation.WEEK_OF_YEAR to "EXTRACT(WEEK FROM %s) + 1",
        TimestampPostOperation.MONTH_OF_YEAR to """
        CASE
            WHEN EXTRACT(MONTH FROM %s) = 1 THEN 'January'
            WHEN EXTRACT(MONTH FROM %<s) = 2 THEN 'February'
            WHEN EXTRACT(MONTH FROM %<s) = 3 THEN 'March'
            WHEN EXTRACT(MONTH FROM %<s) = 4 THEN 'April'
            WHEN EXTRACT(MONTH FROM %<s) = 5 THEN 'May'
            WHEN EXTRACT(MONTH FROM %<s) = 6 THEN 'June'
            WHEN EXTRACT(MONTH FROM %<s) = 7 THEN 'July'
            WHEN EXTRACT(MONTH FROM %<s) = 8 THEN 'August'
            WHEN EXTRACT(MONTH FROM %<s) = 9 THEN 'September'
            WHEN EXTRACT(MONTH FROM %<s) = 10 THEN 'October'
            WHEN EXTRACT(MONTH FROM %<s) = 11 THEN 'November'
            WHEN EXTRACT(MONTH FROM %<s) = 12 THEN 'December'
        END
        """.trimIndent(),
        TimestampPostOperation.QUARTER_OF_YEAR to "CONCAT('Q', CAST(EXTRACT(QUARTER FROM %s) AS STRING))"
    )
}
