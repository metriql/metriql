package com.metriql.jdbc

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.sql.SQLFeatureNotSupportedException
import java.util.Properties
import java.util.regex.Pattern

const val NAME = "metriql"

class MetriqlDriver : Driver {
    override fun connect(url: String?, info: Properties?): Connection {
        TODO("not implemented")
    }

    override fun acceptsURL(url: String): Boolean {
        return url.startsWith(PREFIX)
    }

    override fun getPropertyInfo(url: String, info: Properties): Array<DriverPropertyInfo> {
        val actualDriverUrl = "jdbc:" + url.substring(PREFIX.length).split(":".toRegex(), 2)[0]
        return DriverManager.getDriver(actualDriverUrl).getPropertyInfo(actualDriverUrl, info)
    }

    override fun getMajorVersion() = DRIVER_VERSION_MAJOR
    override fun getMinorVersion() = DRIVER_VERSION_MINOR
    override fun jdbcCompliant() = false
    override fun getParentLogger() = throw SQLFeatureNotSupportedException()

    companion object {
        const val PREFIX = "jdbc:$NAME:"
        private var DRIVER_VERSION: String?
        private var DRIVER_VERSION_MAJOR: Int
        private var DRIVER_VERSION_MINOR: Int

        val version = this::class.java.`package`.implementationVersion
        val matcher = Pattern.compile("^(\\d+)\\.(\\d+)($|[.-])").matcher(version)

        init {
            if (!matcher.find()) {
                DRIVER_VERSION = "unknown"
                DRIVER_VERSION_MAJOR = 0
                DRIVER_VERSION_MINOR = 0
            } else {
                DRIVER_VERSION = version
                DRIVER_VERSION_MAJOR = matcher.group(1).toInt()
                DRIVER_VERSION_MINOR = matcher.group(2).toInt()
            }
        }
    }
}
