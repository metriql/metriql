package com.metriql.util

import com.hubspot.jinjava.Jinjava

object DefaultJinja {
    private val renderer = Jinjava()

    fun renderFunction(template: String, bindings: List<Any?>): String {
        return renderer.render(template, mapOf("value" to bindings))
    }

    fun render(template: String, bindings: Map<String, Any>): String {
        return renderer.render(template, bindings)
    }
}
