package com.metriql.service.auth

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import kotlin.reflect.KClass

data class UserAttributeDefinition(val type: Type, val access: Access, val label: String?, val description: String?) {
    @UppercaseEnum
    enum class Access {
        NONE, VIEW, EDIT;
    }

    @UppercaseEnum
    enum class Type(private val clazz: KClass<out UserAttributeValue<*>>) : StrValueEnum {
        STRING(UserAttributeValue.StringValue::class),
        NUMERIC(UserAttributeValue.NumericValue::class),
        BOOLEAN(UserAttributeValue.BooleanValue::class);

        override fun getValueClass(): Class<*> {
            return this.clazz.java
        }

        sealed class UserAttributeValue<T>(val value: T) {
            class StringValue @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(value: String) : UserAttributeValue<String>(value)
            class NumericValue @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(value: Number) : UserAttributeValue<Number>(value)
            class BooleanValue @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(value: Boolean) : UserAttributeValue<Boolean>(value)

            @JsonValue
            fun value(): T? {
                return value
            }
        }
    }
}
