package com.metriql

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.AnnotationIntrospector
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.PropertyName
import com.fasterxml.jackson.databind.cfg.MapperConfig
import com.fasterxml.jackson.databind.introspect.Annotated
import com.fasterxml.jackson.databind.introspect.AnnotatedClass
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder
import com.metriql.report.data.ReportMetric

class SealedClassInferenceIntrospector : AnnotationIntrospector() {
    override fun findTypeResolver(config: MapperConfig<*>?, ac: AnnotatedClass, baseType: JavaType): TypeResolverBuilder<*>? {
        if (!ac.rawType.kotlin.isSealed) {
            return null
        }
        val b = StdTypeResolverBuilder()
        b.init(JsonTypeInfo.Id.CUSTOM, null)
        b.inclusion(JsonTypeInfo.As.WRAPPER_OBJECT)
        b.typeIdVisibility(false)
        return b
    }

    override fun findSubtypes(a: Annotated): List<NamedType> {
//            JvmClassMappingKt.getKotlinClass(a.getRawType()).getSealedSubclasses().stream().map(subClass -> new NamedType(subClass.getClass(), subClass.getAnnotations().stream().filter(ann -> ann.annotationType() == JsonTypeName.class)));
        return listOf(
            NamedType(ReportMetric.ReportDimension::class.java, "dimension"),
        )
    }

    override fun findPropertyAliases(ann: Annotated): List<PropertyName> {
        val map = ann.rawType.kotlin.sealedSubclasses.flatMap { subClass -> subClass.annotations.mapNotNull { ann -> if (ann is JsonTypeName) ann.value else null } }
            .map { PropertyName(it) }
        return map
    }

    override fun version(): Version {
        return Version.unknownVersion()
    }
}
