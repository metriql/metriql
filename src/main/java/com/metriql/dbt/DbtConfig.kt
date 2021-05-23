package com.metriql.dbt

import io.airlift.configuration.Config

class DbtConfig {
    private var executable: String = "dbt"

    fun getExecutable() = executable

    @Config("develop.dbt.executable-path")
    fun setExecutable(executable: String): DbtConfig {
        this.executable = executable
        return this
    }
}
