package com.metriql.util

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException
import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException

typealias JsonPath = String

object JsonUtil {
    fun convertToUserFriendlyError(e: Exception): Pair<JsonPath?, String> {
        return when (e) {
            is InvalidDefinitionException -> {
                Pair(extractPropertyReference(e.path), e.message ?: "Unknown error")
            }
            is InvalidFormatException -> {
                Pair(removeLastCharacter(extractPropertyReference(e.path)), "Invalid value `${e.value}`")
            }
            is UnrecognizedPropertyException -> {
                Pair(removeLastCharacter(extractPropertyReference(e.path)), "Unknown property")
            }
            is MismatchedInputException -> {
                var reference: String? = null
                if (e.path.isNotEmpty()) {
                    reference = extractPropertyReference(e.path)
                }
                val property = substringBetween(e.localizedMessage, "'", "'")

                if (reference != null && property != null)
                    Pair(reference + property, "Missing property")
                else if (property != null && reference == null)
                    Pair(property, "Missing property")
                else
                    Pair(reference, "Invalid value, cannot parse token `${(e.processor as? JsonParser)?.currentToken}` for target ${e.targetType?.canonicalName}")
            }
            // inherits the other classes
            is JsonMappingException -> {
                Pair(extractPropertyReference(e.path), getExceptionMessage(e.cause) ?: getExceptionMessage(e) ?: "Unknown error")
            }
            else -> Pair(null, e.message ?: "Unknown Error ${e.javaClass.canonicalName}")
        }
    }

    private fun getExceptionMessage(e: Throwable?): String? {
        return when (e) {
            is MetriqlException -> e.toString()
            else -> e?.message
        }
    }

    private fun extractPropertyReference(path: List<JsonMappingException.Reference>): String {
        return path.mapIndexed { idx, reference ->
            when {
                reference.fieldName != null -> {
                    (if (idx > 0) "." else "") + reference.fieldName
                }
                reference.index != null -> "[${reference.index}]"
                else -> "[UNKNOWN(${reference.description})]"
            }
        }.joinToString("")
    }

    // remove '.' at the end of property path reference
    private fun removeLastCharacter(string: String): String? {
        return string.trimEnd('.')
    }

    private fun substringBetween(str: String?, open: String?, close: String?): String? {
        return if (str != null && open != null && close != null) {
            val start = str.indexOf(open)
            if (start != -1) {
                val end = str.indexOf(close, start + open.length)
                if (end != -1) {
                    return str.substring(start + open.length, end)
                }
            }
            null
        } else {
            null
        }
    }
}
