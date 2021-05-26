package com.metriql.service.audit

import io.airlift.configuration.Config

class AuditConfig {
    private var active = true
    private var output = Output.DB

    fun getActive(): Boolean {
        return active
    }

    @Config("audit-active")
    fun setActive(active: Boolean): AuditConfig {
        this.active = active
        return this
    }

    fun getOutput(): Output {
        return output
    }

    @Config("audit-output")
    fun setOutput(output: Output): AuditConfig {
        this.output = output
        return this
    }

    enum class Output {
        DB, CONSOLE
    }
}
