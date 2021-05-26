package com.metriql.warehouse.spi

import com.metriql.service.model.Model
import com.metriql.util.UppercaseEnum

data class DbtSettings(
    val dbType: String,
    val credentials: Map<String, Any>,
    // based on quoting, the reference may be changed
    val tableConfigMapper: (Triple<String, Model.Target.TargetValue.Table?, Map<String, String>?>) -> Map<String, String> = {
        val (name, target, persist) = it

        val table = target ?: Model.Target.TargetValue.Table(null, null, name)

        listOfNotNull(
            if (table.schema != null) "schema" to table.schema else null,
            if (table.database != null) "database" to table.database else null,
            "alias" to table.table
        ).toMap() +
            (persist ?: mapOf())
    }
) {

    companion object {
        fun generateSchemaForModel(baseSchema: String?, targetSchema: String?): String? {
            return if (baseSchema?.isNotEmpty() == true && targetSchema?.isNotEmpty() == true) {
                "${baseSchema}_$targetSchema"
            } else if (baseSchema?.isNotEmpty() == true) {
                baseSchema
            } else if (targetSchema?.isNotEmpty() == true) {
                targetSchema
            } else {
                null
            }
        }
    }
}

@UppercaseEnum
enum class DBTType {
    VIEW, TABLE, INCREMENTAL;
}
