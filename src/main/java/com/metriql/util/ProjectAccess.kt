package com.metriql.util

import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy.RUNTIME

@Retention(RUNTIME)
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER, AnnotationTarget.PROPERTY_SETTER)
annotation class ProjectAccess(val permissions: IntArray = [])
