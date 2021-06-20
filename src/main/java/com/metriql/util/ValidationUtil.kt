package com.metriql.util

import java.net.Inet4Address
import java.net.InetAddress
import java.net.UnknownHostException

object ValidationUtil {
    @JvmStatic
    fun quoteIdentifier(reference: String, character: Char? = '"'): String {
        if (character == null) {
            return reference
        }

        return character + reference.replace(character.toString().toRegex(), "") + character
    }

    @JvmStatic
    fun stripLiteral(value: String): String {
        return value.replace("'".toRegex(), "''")
    }

    @JvmStatic
    fun checkForPrivateIPAccess(host: String): String? {
        val address = try {
            InetAddress.getByName(host) as Inet4Address
        } catch (exception: UnknownHostException) {
            return null
        }

        if (address.isLoopbackAddress || address.isLinkLocalAddress) {
            return "Loopback addresses are not supported."
        }

        if (address.isSiteLocalAddress || address.isAnyLocalAddress || address.isMulticastAddress) {
            return "Local addresses are not supported"
        }

        return null
    }
}
