package com.metriql.warehouse.spi

import com.metriql.db.FieldType
import com.metriql.report.data.recipe.Recipe

typealias DatabaseName = String
typealias SchemaName = String
typealias TableName = String
typealias ColumnName = String

data class TableSchema(val name: TableName, val comment: String?, val columns: List<Column>) {
    data class Column(val name: ColumnName, val sql: String?, val dbType: String?, val type: FieldType?, val label: String?) {
        fun toDimension(): Recipe.RecipeModel.Metric.RecipeDimension {
            return Recipe.RecipeModel.Metric.RecipeDimension(column = name, type = type, description = label)
        }
    }
}
