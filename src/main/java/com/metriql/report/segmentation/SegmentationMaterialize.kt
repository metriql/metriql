package com.metriql.report.segmentation

import com.metriql.report.data.ReportFilter
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.model.DatasetName
import com.metriql.warehouse.spi.services.MaterializeQuery
import com.metriql.warehouse.spi.services.ServiceQuery

data class SegmentationMaterialize(
    val measures: List<Recipe.FieldReference>,
    val dimensions: List<Recipe.FieldReference>?,
    val filters: ReportFilter?,
    val tableName: String? = null
) : MaterializeQuery {
    override fun toQuery(modelName: DatasetName): ServiceQuery {
        return SegmentationQuery(modelName, measures, dimensions, filters)
    }
}
