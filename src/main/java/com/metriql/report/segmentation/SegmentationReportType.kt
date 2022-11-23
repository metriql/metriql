package com.metriql.report.segmentation

import com.metriql.report.ReportType
import com.metriql.report.ReportTypeProxy

class SegmentationReportTypeProxy : ReportType by SegmentationReportType, ReportTypeProxy(SegmentationReportType)

object SegmentationReportType : ReportType {
    override val slug = "segmentation"
    override val dataClass = SegmentationQuery::class
    override val serviceClass = SegmentationService::class
}
