package com.metriql.warehouse.spi.services.segmentation

import com.metriql.report.segmentation.SegmentationQuery
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.services.ServiceQueryDSL
import com.metriql.warehouse.spi.services.ServiceQueryGenerator
import com.metriql.warehouse.spi.services.ServiceSupport

typealias SegmentationQueryGenerator = ServiceQueryGenerator<Segmentation, SegmentationQuery, ServiceSupport>

data class Segmentation(
    val columnNames: List<String>,
    val dimensions: List<WarehouseMetriqlBridge.RenderedField>,
    val measures: List<WarehouseMetriqlBridge.RenderedField>,
    val tableReference: String,
    val limit: Int?,
    val tableAlias: String,
    val joins: Set<String>?,
    val whereFilter: String?,
    val groups: Set<String>?,
    val groupIdx: Set<Int>?,
    val havingFilter: String?,
    val orderByIdx: Set<String>?,
    val orderBy: Set<String>?
) : ServiceQueryDSL
