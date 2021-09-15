package com.metriql.util

import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.common.base.CaseFormat
import net.gcardone.junidecode.Junidecode
import java.math.BigInteger
import java.security.MessageDigest
import java.text.Normalizer
import java.util.Locale
import java.util.regex.Pattern
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor

object TextUtil {
    private val modelReplaceRegex = "[^a-z_]+[^a-z0-9_]*".toRegex()

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

    fun toMetriqlConventionalName(name: String): String {
        val preProcessed = Junidecode.unidecode(name)
            .trim()
            .replace(" ", "_")
            .lowercase(Locale.ENGLISH)
        return preProcessed
            .replace("[0-9]+$".toRegex(), "_")
            .replace("^[0-9]+".toRegex(), "_")
            .replace("[^a-z0-9_]+".toRegex(), "_")
            .take(120)
    }

    fun toSlug(input: String, useUnderscore: Boolean = false): String {
        val nowhitespace = WHITESPACE.matcher(input).replaceAll(if (useUnderscore) "_" else "-")
        val normalized: String = Normalizer.normalize(nowhitespace, Normalizer.Form.NFD)
        val slug = (if (useUnderscore) NONLATIN_UNDERSCORE else NONLATIN_NORMAL).matcher(normalized).replaceAll(if (useUnderscore) "_" else "-")
        return slug.lowercase(Locale.ENGLISH)
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
