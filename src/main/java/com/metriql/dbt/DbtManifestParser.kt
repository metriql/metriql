package com.metriql.dbt

import com.fasterxml.jackson.databind.DeserializationFeature
import com.metriql.report.data.recipe.Recipe
import com.metriql.util.JsonHelper

object DbtManifestParser {
    val mapper = JsonHelper.getMapper().copy()!!

    init {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, false)
        mapper.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
    }

    fun parse(manifestFile: ByteArray): List<Recipe.RecipeModel> {
        val manifest = mapper.readValue(manifestFile, DbtManifest::class.java)
        val models = manifest.nodes.mapNotNull { it.value.toModel(manifest) }
        val sources = manifest.sources.mapNotNull { it.value.toModel(manifest) }
        return models + sources
    }
}
