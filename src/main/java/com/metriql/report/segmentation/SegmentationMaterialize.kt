package com.metriql.report.segmentation

import com.metriql.report.data.ReportFilter
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.model.DatasetName
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.warehouse.spi.function.TimestampPostOperation
import com.metriql.warehouse.spi.services.MaterializeQuery
import com.metriql.warehouse.spi.services.ServiceQuery
import io.netty.handler.codec.http.HttpResponseStatus

data class SegmentationMaterialize(
    val measures: List<Recipe.FieldReference>,
    val dimensions: List<Recipe.FieldReference>?,
    val filters: ReportFilter?,
    val tableName: String? = null
) : MaterializeQuery {
    override fun toQuery(modelName: DatasetName): ServiceQuery {
        return SegmentationQuery(modelName, measures, dimensions, filters)
    }

    override fun check() {
        val hasEventTimestampDimension = dimensions?.any {
            // TODO: if the dimension type is timestamp
            if (false) {
                val timeframe = it.timeframe ?: throw MetriqlException(
                    "Timeframe is required for ${it.name} dimension",
                    HttpResponseStatus.BAD_REQUEST
                )
                val enum = JsonHelper.convert(timeframe, TimestampPostOperation::class.java)
                if (enum != TimestampPostOperation.HOUR && !enum.isInclusive(TimestampPostOperation.YEAR)) {
                    throw MetriqlException(
                        "One of HOUR, DAY, WEEK, MONTH or YEAR timeframe of the eventTimestamp is required for incremental models",
                        HttpResponseStatus.BAD_REQUEST
                    )
                }

                true
            } else {
                false
            }
        } ?: false
    }
}
