package com.metriql.warehouse.duckdb

import com.metriql.report.segmentation.SegmentationReportType
import com.metriql.warehouse.postgresql.BasePostgresqlMetriqlBridge
import com.metriql.warehouse.postgresql.PostgresqlFilters
import com.metriql.warehouse.postgresql.PostgresqlMetriqlBridge
import com.metriql.warehouse.postgresql.PostgresqlTimeframes
import com.metriql.warehouse.spi.services.segmentation.ANSISQLSegmentationQueryGenerator
import io.trino.spi.type.StandardTypes

object DuckdbMetriqlBridge : BasePostgresqlMetriqlBridge() {
    override val filters = PostgresqlFilters { PostgresqlMetriqlBridge }
    override val timeframes = PostgresqlTimeframes()

    override val queryGenerators = mapOf(
        SegmentationReportType.slug to ANSISQLSegmentationQueryGenerator(),
    )

    override val mqlTypeMap = super.mqlTypeMap + mapOf(
        StandardTypes.DOUBLE to "DOUBLE PRECISION"
    )
}
