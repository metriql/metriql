package com.metriql.util

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import com.metriql.warehouse.spi.function.DatePostOperation
import com.metriql.warehouse.spi.function.IPostOperation
import com.metriql.warehouse.spi.function.TimePostOperation
import com.metriql.warehouse.spi.function.TimestampPostOperation
import io.netty.handler.codec.http.HttpResponseStatus
import java.time.temporal.ChronoUnit
import java.util.regex.Pattern

class RPeriod @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(rawValue: String) {

    var type: Type
    var value: Int

    init {
        val possiblePeriod = periodMatcher.matcher(rawValue)
        if (!possiblePeriod.matches()) {
            throw MetriqlException("Period value $rawValue is not valid", HttpResponseStatus.BAD_REQUEST)
        }

        this.value = Integer.parseInt(possiblePeriod.group("num"))
        this.type = Type.parse(possiblePeriod.group("period"))
    }

    @JsonValue
    fun getValue() = "P$value${type.serializedAlias}"

    companion object {
        fun fromName(s: String): RPeriod {
            return RPeriod(s)
        }

        private var periodMatcher = Pattern.compile("p?(?<num>\\d+)( *)?(?<period>\\w+)", Pattern.CASE_INSENSITIVE)
    }

    enum class Type(val aliases: Set<String>, val serializedAlias: String, val postOperation: Set<IPostOperation>, val unit: ChronoUnit) {
        MINUTE(
            setOf("minutes", "minute"),
            "MIN",
            setOf(TimePostOperation.MINUTE),
            ChronoUnit.MINUTES
        ),
        HOUR(
            setOf("hour", "hours", "h"),
            "H",
            setOf(TimestampPostOperation.HOUR),
            ChronoUnit.HOURS
        ),
        DAY(
            setOf("day", "days", "d"),
            "D",
            setOf(TimestampPostOperation.DAY, DatePostOperation.DAY),
            ChronoUnit.DAYS
        ),
        WEEK(
            setOf("week", "weeks", "w"),
            "W",
            setOf(TimestampPostOperation.WEEK, DatePostOperation.WEEK),
            ChronoUnit.WEEKS
        ),
        MONTH(
            setOf("month", "months", "m"),
            "M",
            setOf(TimestampPostOperation.MONTH, DatePostOperation.MONTH),
            ChronoUnit.MONTHS
        ),
        YEAR(
            setOf("year", "years", "y"),
            "Y",
            setOf(TimestampPostOperation.YEAR, DatePostOperation.YEAR),
            ChronoUnit.YEARS
        );

        companion object {
            fun parse(value: String): Type {
                val lowerCaseValue = value.toLowerCase()
                return values().find { it.aliases.contains(lowerCaseValue) }
                    ?: throw MetriqlException("Bad date input $value", HttpResponseStatus.BAD_REQUEST)
            }
        }
    }
}
