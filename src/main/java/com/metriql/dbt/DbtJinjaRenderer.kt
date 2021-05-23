package com.metriql.dbt

import com.fasterxml.jackson.core.type.TypeReference
import com.hubspot.jinjava.Jinjava
import com.hubspot.jinjava.JinjavaConfig
import com.hubspot.jinjava.el.ext.NamedParameter
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.hubspot.jinjava.interpret.RenderResult
import com.metriql.util.CryptUtil
import com.metriql.util.JsonHelper
import com.metriql.util.TextUtil
import com.metriql.util.ValidationUtil
import com.metriql.warehouse.spi.DataSource
import jinjava.javax.el.ELException
import java.io.File
import java.lang.reflect.Method

class DbtJinjaRenderer(macros: String = "") {
    val jinjava = Jinjava(
        JinjavaConfig.newBuilder()
            .withFailOnUnknownTokens(false)
            .build()
    )

    init {
        val methods: Array<Method> = DbtFunctions::class.java.declaredMethods
        methods.forEach {
            jinjava.globalContext.registerFunction(com.hubspot.jinjava.lib.fn.ELFunctionDefinition("", it.name, it))
        }

        jinjava.render(macros, mapOf<String, Any>())
    }

    companion object {
        val renderer = DbtJinjaRenderer()

        // use the delimiter in order to parse configs
        val delimiter: String = CryptUtil.generateRandomKey(16)
    }

    fun getReferenceLabel(content: String, packageName: String): String {
        return jinjava.render(
            content,
            mapOf(
                "_package_name" to packageName,
                "_label" to true,
            )
        )
    }

    fun renderReference(content: String, packageName: String): String {
        return jinjava.render(
            content,
            mapOf(
                "_package_name" to packageName,
            )
        )
    }

    fun render(
        dataSource: DataSource,
        content: String,
        projectPath: File,
        packageName: String,
        relativePath: String,
        vars: Map<String, Any?>,
        files: Map<String, String>
    ): RenderResult {
        return jinjava.renderForResult(
            content,
            mapOf(
                "_package_name" to packageName,
                "_vars" to vars,
                "_files" to files,
                "_db" to dataSource,
                "_project_path" to projectPath,
                "_current_file" to relativePath,
            )
        )
    }

    data class Target(val database: String?, val schema: String?)

    object DbtFunctions {
        @JvmStatic
        fun `var`(vararg arguments: Any?): Any? {
            val vars = JinjavaInterpreter.getCurrent().context["_vars"]
            if (vars !is Map<*, *>) {
                throw IllegalStateException()
            }

            return vars[arguments[0]]
        }

        fun generate_source(vararg arguments: Any?): DbtSchemaYaml.DbtSource {
            return DbtSchemaYaml.DbtSource("null", null, null, null, null, null, null, null)
        }

        fun run_query(vararg arguments: Any?): List<Map<String, Any?>> {
            val sql = arguments[0].toString()
            return listOf()
        }

        @JvmStatic
        fun get_alias(vararg arguments: Any?): Any? {
            val context = JinjavaInterpreter.getCurrent().context
            val dataSource = context["_db"] as DataSource?
            val refs = JsonHelper.read(arguments[0].toString(), object : TypeReference<Map<String, String>>() {})
            val aliasQuote = dataSource!!.warehouse.bridge.aliasQuote

            if (refs == null) {
                throw ELException("get_alias requires a valid relation argument. Ex: {database: null, schema: null, table: null}")
            }
            return listOfNotNull(
                refs["database"]?.let { ValidationUtil.quoteIdentifier(it, aliasQuote) },
                refs["schema"]?.let { ValidationUtil.quoteIdentifier(it, aliasQuote) },
                refs["table"]?.let { ValidationUtil.quoteIdentifier(it, aliasQuote) }
            ).joinToString(".")
        }

        @JvmStatic
        fun source(vararg args: String?): String {
            val context = JinjavaInterpreter.getCurrent().context
            val packageName = context["_package_name"] as String

            val name = args[1]!!
            val sourceName = args[0]!!

            if (context["_label"] == true) {
                return name
            }

            val modelName = DbtManifest.getModelName("source", packageName, "${sourceName}_$name")

            return modelName
            // ugly hack in order to find out we're compiling jinja files
//            return if (context["_project_path"] != null) "{.{model.$modelName}}" else modelName
        }

        @JvmStatic
        fun generate_slug(vararg arguments: Any?): String {
            return TextUtil.toSlug(arguments[0]?.toString() ?: throw IllegalArgumentException("`generate_slug` first argument must be a string"), true)
        }

        @JvmStatic
        fun view(vararg arguments: Any?): String {
            val output = JsonHelper.encode(
                arguments.mapNotNull {
                    when (it) {
                        is NamedParameter -> it
                        is String -> NamedParameter("name", it)
                        else -> null
                    }
                }.map { it.name to it.value }.toMap()
            )

            return delimiter + output
        }

        @JvmStatic
        fun env_var(vararg args: Array<out String?>): String? {
            return System.getenv(args[0].toString())
        }

        @JvmStatic
        fun ref(vararg args: String?): String {
            val context = JinjavaInterpreter.getCurrent().context
            val packageName = context["_package_name"] as String

            val model = args[0]!!
            if (context["_label"] == true) {
                return model
            }

            val modelName = DbtManifest.getModelName("model", packageName, model)
            return modelName
            // ugly hack in order to find out we're compiling jinja files
//            return if (context["_project_path"] != null) "{{model.$modelName}}" else modelName
        }

        @JvmStatic
        fun import(vararg path: String): String {
            val context = JinjavaInterpreter.getCurrent().context
            val files = context["_files"]
            val projectPath = context["_project_path"] as File
            val current = context["_current_file"] as String
            if (files !is Map<*, *>) {
                throw IllegalStateException()
            }

            val targetFile = File(File(projectPath, current).parent, path[0]).toPath().normalize().toAbsolutePath().toUri().path
            val relativePath = targetFile.substring(projectPath.path.length + 1)
            return files[relativePath]?.toString() ?: throw ELException("File `$relativePath` could not found.")
        }
    }
}
