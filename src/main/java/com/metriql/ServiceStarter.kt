package com.metriql

import com.github.ajalt.clikt.core.subcommands
import java.sql.DriverManager
import java.sql.SQLException

fun main(args: Array<String>) { // Intercepting redshift and postgresql drivers
    fixRedshift()
    Commands()
        .subcommands(Commands.Generate(), Commands.Serve()).main(args)
}

fun fixRedshift() {
    // https://stackoverflow.com/questions/31951518/redshift-and-postgres-jdbc-driver-both-intercept-jdbc-postgresql-connection-st
    val drivers = DriverManager.getDrivers()
    while (drivers.hasMoreElements()) {
        val d = drivers.nextElement()
        if (d.javaClass.name == "com.amazon.redshift.jdbc.Driver") {
            try {
                DriverManager.deregisterDriver(d)
                DriverManager.registerDriver(d)
            } catch (e: SQLException) {
                throw RuntimeException("Could not deregister redshift driver")
            }

            break
        }
    }
}
