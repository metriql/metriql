package com.metriql.warehouse

import com.metriql.db.FieldType
import com.metriql.service.model.Model
import com.metriql.warehouse.spi.DBTType
import com.metriql.warehouse.spi.filter.AnyOperatorType
import com.metriql.warehouse.spi.filter.ArrayOperatorType
import com.metriql.warehouse.spi.filter.BooleanOperatorType
import com.metriql.warehouse.spi.filter.DateOperatorType
import com.metriql.warehouse.spi.filter.NumberOperatorType
import com.metriql.warehouse.spi.filter.StringOperatorType
import com.metriql.warehouse.spi.filter.TimeOperatorType
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import com.metriql.warehouse.spi.services.ServiceSupport

data class WarehouseSupports(
    val filters: Filters,
    val postOperations: PostOperations,
    val dbtTypes: Set<DBTType>,
    val services: Map<String, List<ServiceSupport>>,
    val aliasQuote: Char?,
    val aggregations: List<Model.Measure.AggregationType>
) {
    data class Filters(
        val any: Set<AnyOperatorType>,
        val string: Set<StringOperatorType>,
        val boolean: Set<BooleanOperatorType>,
        val number: Set<NumberOperatorType>,
        val time: Set<TimeOperatorType>,
        val timestamp: Set<TimestampOperatorType>,
        val date: Set<DateOperatorType>,
        val array: Set<ArrayOperatorType>
    )

    data class PostOperation(
        val value: String,
        val valueType: FieldType,
        val category: String?
    )

    data class PostOperations(val timestamp: List<PostOperation>, val date: List<PostOperation>, val time: List<PostOperation>)
}
