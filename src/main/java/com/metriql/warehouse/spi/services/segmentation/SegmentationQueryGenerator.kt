package com.metriql.warehouse.spi.services.segmentation

import com.metriql.report.segmentation.SegmentationReportOptions
import com.metriql.warehouse.spi.services.ServiceQueryDSL
import com.metriql.warehouse.spi.services.ServiceQueryGenerator
import com.metriql.warehouse.spi.services.ServiceSupport

typealias SegmentationQueryGenerator = ServiceQueryGenerator<Segmentation, SegmentationReportOptions, ServiceSupport>

data class Segmentation(
    val projections: List<String>,
    val tableReference: String,
    val tableAlias: String,
    val joins: Set<String>?,
    val whereFilters: List<String>?,
    val groups: Set<String>?,
    val groupIdx: Set<Int>?,
    val havingFilters: Set<String>?,
    val orderBy: Set<String>?
) : ServiceQueryDSL
