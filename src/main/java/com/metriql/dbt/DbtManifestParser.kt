package com.metriql.dbt

import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver
import com.fasterxml.jackson.databind.util.LinkedNode
import com.metriql.report.data.recipe.Recipe
import com.metriql.util.JsonHelper

object DbtManifestParser {
    val mapper = JsonHelper.getMapper().copy()!!

    init {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, false)
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE)

        // An ugly hack for dbt tests as they can be custom as well
        val deserializationConfig = mapper.deserializationConfig.withHandler(object : DeserializationProblemHandler() {
            override fun handleUnknownTypeId(ctxt: DeserializationContext, baseType: JavaType, subTypeId: String, idResolver: TypeIdResolver, failureMsg: String?): JavaType {
                return if(baseType.isTypeOrSubTypeOf(DbtManifest.Node.TestMetadata.DbtModelColumnTest::class.java)) {
                    ctxt.constructType(Void::class.java)
                } else {
                    super.handleUnknownTypeId(ctxt, baseType, subTypeId, idResolver, failureMsg)
                }
            }
        })

       mapper.setConfig(deserializationConfig)

    }

    fun parse(manifestFile: ByteArray): List<Recipe.RecipeModel> {
        val manifest = mapper.readValue(manifestFile, DbtManifest::class.java)
        val models = manifest.nodes.mapNotNull { it.value.toModel(manifest) }
        val sources = manifest.sources.mapNotNull { it.value.toModel(manifest) }
        return models + sources
    }
}
