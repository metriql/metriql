package com.metriql.report.data

import com.metriql.report.segmentation.SegmentationRecipeQuery

typealias ReportFilters = List<ReportFilter>
//class ReportFilters(val connector : ReportFilter.FilterValue.MetricFilter.Connector, val items : List<ReportFilter>) {
//
//    fun toRecipeFilters(): SegmentationRecipeQuery.Filters {
//        return SegmentationRecipeQuery.Filters(connector, items.mapNotNull { it.toReference() })
//    }
//
//    companion object {
//        fun single(filter : ReportFilter) =
//            ReportFilters(ReportFilter.FilterValue.MetricFilter.Connector.AND, listOf(filter))
//        val EMPTY = ReportFilters(ReportFilter.FilterValue.MetricFilter.Connector.AND, listOf())
//    }
//}
