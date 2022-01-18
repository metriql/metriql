package com.metriql.report.segmentation

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy
import com.metriql.report.sql.SqlReportType

class SegmentationReportTypeProxy : ReportType by SegmentationReportType, ReportTypeProxy(SqlReportType)

object SegmentationReportType : ReportType {
    override val slug = "segmentation"
    override val configClass = SegmentationReportOptions::class
    override val recipeClass = SegmentationRecipeQuery::class
    override val serviceClass = SegmentationService::class
}
