package com.metriql.report.retention

import com.metriql.report.RecipeDataset
import com.metriql.service.model.DimensionName
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.services.RecipeQuery
import com.metriql.warehouse.spi.services.ServiceReportOptions

class RetentionRecipeQuery(
    val firstStep: RecipeDataset,
    val returningStep: RecipeDataset,
    val dimension: DimensionName?,
    val approximate: Boolean,
    val dateUnit: RetentionReportOptions.DateUnit,
    val connector: DimensionName?
) : RecipeQuery {

    override fun toReportOptions(context: IQueryGeneratorContext): ServiceReportOptions {
        return RetentionReportOptions(firstStep.toDataset(context), returningStep.toDataset(context), null, dimension, approximate, dateUnit, connector)
    }
}
