package com.metriql.report

import com.metriql.report.flow.FlowReportOptions
import com.metriql.report.flow.FlowService
import com.metriql.report.funnel.FunnelRecipeQuery
import com.metriql.report.funnel.FunnelReportOptions
import com.metriql.report.funnel.FunnelService
import com.metriql.report.mql.MqlReportOptions
import com.metriql.report.retention.RetentionRecipeQuery
import com.metriql.report.retention.RetentionReportOptions
import com.metriql.report.retention.RetentionService
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.report.segmentation.SegmentationReportOptions
import com.metriql.report.segmentation.SegmentationService
import com.metriql.report.mql.MqlService
import com.metriql.report.sql.SqlReportOptions
import com.metriql.report.sql.SqlService
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions
import kotlin.reflect.KClass

@UppercaseEnum
enum class ReportType(
    val configClass: KClass<out ServiceReportOptions>,
    val recipeClass: KClass<out RecipeQuery>,
    val materializeClass: KClass<out SegmentationRecipeQuery.SegmentationMaterialize>?,
    val serviceClass: KClass<out IAdHocService<out ServiceReportOptions>>
) : StrValueEnum {
    SEGMENTATION(SegmentationReportOptions::class, SegmentationRecipeQuery::class, null, SegmentationService::class),
    FUNNEL(FunnelReportOptions::class, FunnelRecipeQuery::class, null, FunnelService::class),
    FLOW(FlowReportOptions::class, SegmentationRecipeQuery::class, null, FlowService::class),
    RETENTION(RetentionReportOptions::class, RetentionRecipeQuery::class, null, RetentionService::class),
    SQL(SqlReportOptions::class, SqlReportOptions::class, null, SqlService::class),
    MQL(SqlReportOptions::class, MqlReportOptions::class, null, MqlService::class);

    override fun getValueClass(): Class<*> {
        return configClass.java
    }

    override fun getValueClass(name: String): Class<*> {
        return when (name) {
            "recipe" -> recipeClass.java
            else -> throw UnsupportedOperationException("Invalid param $name")
        }
    }
}
