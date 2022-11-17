package com.metriql.report.jinja

data class JinjaApps(val apps: List<JinjaApp>) {
    data class JinjaApp(val raw_code : String, val config : Config?) {
        data class Config(val method: kong.unirest.HttpMethod?)
    }
}
