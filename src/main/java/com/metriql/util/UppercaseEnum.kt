package com.metriql.util

import com.fasterxml.jackson.annotation.JacksonAnnotation
import java.lang.annotation.Inherited

@Target(AnnotationTarget.CLASS, AnnotationTarget.FILE)
@Inherited
@JacksonAnnotation
annotation class UppercaseEnum
