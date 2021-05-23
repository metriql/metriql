package com.metriql.dbt

import com.fasterxml.jackson.databind.node.ObjectNode

class DbtProfiles(val config: Config?) : HashMap<String, DbtProfiles.Profile>() {
    data class Config(val send_anonymous_usage_stats: Boolean?)

    data class Profile(val target: String?, val outputs: TargetMap)
    class TargetMap : HashMap<String, ObjectNode>()
}
