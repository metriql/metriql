package com.metriql.util

import com.fasterxml.jackson.annotation.JacksonAnnotation
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.lang.annotation.Inherited
import kotlin.reflect.KClass

@Target(
    AnnotationTarget.ANNOTATION_CLASS,
    AnnotationTarget.CLASS,
    AnnotationTarget.FILE,
    AnnotationTarget.FIELD,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.PROPERTY_GETTER,
    AnnotationTarget.PROPERTY_SETTER,
    AnnotationTarget.VALUE_PARAMETER
)

@Inherited
@JacksonAnnotation
annotation class PolymorphicTypeStr<E>(
    val externalProperty: String,
    val inclusion: JsonTypeInfo.As = JsonTypeInfo.As.EXTERNAL_PROPERTY,
    val valuesEnum: KClass<E>,
    val isNamed: Boolean = false,
    val name: String = ""
) where E : Enum<E>, E : StrValueEnum

interface StrValueEnum {
    fun getValueClass(): Class<*>
    fun getValueClass(parameter: String): Class<*> {
        throw UnsupportedOperationException()
    }
}

@Target(
    AnnotationTarget.ANNOTATION_CLASS,
    AnnotationTarget.CLASS,
    AnnotationTarget.FILE,
    AnnotationTarget.FIELD,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.PROPERTY_GETTER,
    AnnotationTarget.PROPERTY_SETTER,
    AnnotationTarget.VALUE_PARAMETER
)
@Inherited
@JacksonAnnotation
annotation class PolymorphicTypeInt<E>(
    val externalProperty: String,
    val inclusion: JsonTypeInfo.As = JsonTypeInfo.As.EXTERNAL_PROPERTY,
    val valuesEnum: KClass<E>
) where E : Enum<E>, E : IntValueEnum

interface IntValueEnum {
    fun getName(): Int
    fun getIntValueClass(): Class<*>
}
