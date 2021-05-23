package com.metriql.db

import java.lang.annotation.Inherited
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy

/*
    JDBI Mappers make use of this annotation for mapping the classes into JSONB types
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS, AnnotationTarget.TYPEALIAS)
@Inherited
annotation class JSONBSerializable
