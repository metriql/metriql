package com.metriql.util

import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.common.base.CaseFormat
import java.math.BigInteger
import java.security.MessageDigest
import java.text.Normalizer
import java.util.Locale
import java.util.regex.Pattern
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor

object TextUtil {
    fun md5(input: String): String {
        val md = MessageDigest.getInstance("MD5")
        return BigInteger(1, md.digest(input.toByteArray())).toString(16).padStart(32, '0')
    }

    fun version(): String {
        return this.javaClass.getPackage().implementationVersion ?: "(unknown)"
    }

    fun toUserFriendly(input: String): String {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, input).replace(
            String.format(
                "%s|%s|%s",
                "(?<=[A-Z])(?=[A-Z][a-z])",
                "(?<=[^A-Z])(?=[A-Z])",
                "(?<=[A-Za-z])(?=[^A-Za-z])"
            ).toRegex(),
            " "
        )
    }

    fun toSlug(input: String, useUnderscore: Boolean = false): String {
        val nowhitespace = WHITESPACE.matcher(input).replaceAll(if (useUnderscore) "_" else "-")
        val normalized: String = Normalizer.normalize(nowhitespace, Normalizer.Form.NFD)
        val slug = (if (useUnderscore) NONLATIN_UNDERSCORE else NONLATIN_NORMAL).matcher(normalized).replaceAll(if (useUnderscore) "_" else "-")
        return slug.toLowerCase(Locale.ENGLISH)
    }

    private val NONLATIN_NORMAL: Pattern = Pattern.compile("[^\\w-]")
    private val NONLATIN_UNDERSCORE: Pattern = Pattern.compile("[^\\w_]")
    private val WHITESPACE: Pattern = Pattern.compile("[\\s]")

    val resourceRegex = "^[a-z_][a-z0-9_]{0,120}\$".toRegex()
}

@JsonIgnore
inline infix fun <reified T : Any> T.merge(other: T): T {
    val propertiesByName = T::class.declaredMemberProperties.associateBy { it.name }
    val primaryConstructor = T::class.primaryConstructor
        ?: throw IllegalArgumentException("merge type must have a primary constructor")
    val args = primaryConstructor.parameters.associateWith { parameter ->
        val property = propertiesByName[parameter.name]
            ?: throw IllegalStateException("no declared member property found with name '${parameter.name}'")
        (property.get(this) ?: property.get(other))
    }
    return primaryConstructor.callBy(args)
}
