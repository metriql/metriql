package com.metriql.dbt

import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver
import com.metriql.report.data.recipe.Recipe
import com.metriql.util.JsonHelper
import com.metriql.util.JsonUtil.convertToUserFriendlyError
import com.metriql.warehouse.spi.DataSource

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
                return if (baseType.isTypeOrSubTypeOf(DbtManifest.Node.TestMetadata.DbtModelColumnTest::class.java)) {
                    ctxt.constructType(Void::class.java)
                } else {
                    super.handleUnknownTypeId(ctxt, baseType, subTypeId, idResolver, failureMsg)
                }
            }
        })

        mapper.setConfig(deserializationConfig)
    }

    fun parse(dataSource: DataSource, manifestFile: ByteArray, modelsFilterOptional: String?): List<Recipe.RecipeModel> {
        val manifest = try {
            mapper.readValue(manifestFile, DbtManifest::class.java)
        } catch (e: Exception) {
            throw DbtYamlParser.ParseException(convertToUserFriendlyError(e))
        }
        modelsFilterOptional?.let { modelsFilter ->
            modelsFilter.split(" ").map {
                val typeAndValue = it.split(":".toRegex(), 2)

                val value = if (typeAndValue.size == 1) typeAndValue[0] else typeAndValue[1]
                val type = if (typeAndValue.size == 1) null else typeAndValue[0]
            }
        }
        val models = manifest.nodes.mapNotNull { it.value.toModel(dataSource, manifest) }
        val sources = manifest.sources.mapNotNull { it.value.toModel(manifest) }
        val modelsAndSources = models + sources
        val metrics = manifest.metrics.mapNotNull { it.value.toModel(manifest, modelsAndSources) }
        return modelsAndSources + metrics
    }

    enum class MatchType {
        path, tag, config
    }
}
