package com.metriql.dbt

import com.fasterxml.jackson.core.type.TypeReference
import com.hubspot.jinjava.Jinjava
import com.hubspot.jinjava.JinjavaConfig
import com.hubspot.jinjava.el.ext.NamedParameter
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.hubspot.jinjava.interpret.RenderResult
import com.metriql.service.model.ModelName
import com.metriql.util.CryptUtil
import com.metriql.util.JsonHelper
import com.metriql.util.TextUtil
import com.metriql.warehouse.spi.DataSource
import jinjava.javax.el.ELException
import java.io.File
import java.lang.reflect.Method

const val IS_MATCH = "_match"

class DbtJinjaRenderer {
    val jinjava = Jinjava(
        JinjavaConfig.newBuilder()
            .build()
    )

    init {
        val methods: Array<Method> = DbtFunctions::class.java.declaredMethods
        methods.forEach {
            jinjava.globalContext.registerFunction(com.hubspot.jinjava.lib.fn.ELFunctionDefinition("", it.name, it))
        }
    }

    companion object {
        val renderer = DbtJinjaRenderer()

        // use the delimiter in order to parse configs
        val delimiter: String = CryptUtil.generateRandomKey(16)
    }

    fun renderModelNameRegex(dataset: ModelName): ModelName {
        return if (dataset.startsWith("ref(") || dataset.startsWith("source(") || dataset.startsWith("metric(")) {
            jinjava.render(
                "{{$dataset}}",
                mapOf(IS_MATCH to true)
            )
        } else {
            dataset
        }
    }

    fun renderReference(content: String, packageName: String): String {
        return jinjava.render(
            content,
            mapOf(
                "_package_name" to packageName,
            )
        )
    }

    fun renderProfiles(content: String, vars: Map<String, Any?>): String? {
        return jinjava.render(content, mapOf("_vars" to vars))
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
                ?: throw ELException("get_alias requires a valid relation argument. Ex: {database: null, schema: null, table: null}")

            return listOfNotNull(
                refs["database"]?.let { dataSource!!.warehouse.bridge.quoteIdentifier(it) },
                refs["schema"]?.let { dataSource!!.warehouse.bridge.quoteIdentifier(it) },
                refs["table"]?.let { dataSource!!.warehouse.bridge.quoteIdentifier(it) }
            ).joinToString(".")
        }

        @JvmStatic
        fun source(vararg args: String?): String {
            val context = JinjavaInterpreter.getCurrent().context
            var packageName = if (args.size > 2) args[0]!! else context["_package_name"] as String?

            val hasPackageNameAsArgument = args.size > 2
            val name = if (hasPackageNameAsArgument) args[2]!! else args[1]!!
            val sourceName = "${if (hasPackageNameAsArgument) args[1]!! else args[0]!!}_$name"

            return when {
                context[IS_MATCH] == true -> DbtManifest.getModelNameRegex("source", sourceName, packageName)
                else -> DbtManifest.getModelName("source", sourceName, packageName!!)
            }
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
        fun env_var(vararg args: String?): String? {
            return System.getenv(args[0].toString())
        }

        @JvmStatic
        fun ref(vararg args: String?): String {
            val context = JinjavaInterpreter.getCurrent().context
            val packageName = if (args.size > 1) args[0]!! else context["_package_name"] as String?

            val model = if (args.size == 1) args[0]!! else args[1]!!

            return when {
                context[IS_MATCH] == true -> {
                    DbtManifest.getModelNameRegex("model", model, packageName)
                }
                else -> {
                    DbtManifest.getModelName("model", model, packageName!!)
                }
            }
        }

        @JvmStatic
        fun metric(vararg args: String?): String {
            val context = JinjavaInterpreter.getCurrent().context
            val packageName = if (args.size > 1) args[0]!! else context["_package_name"] as String?

            val model = if (args.size == 1) args[0]!! else args[1]!!

            return when {
                context[IS_MATCH] == true -> {
                    DbtManifest.getModelNameRegex("metric", model, packageName)
                }
                else -> {
                    DbtManifest.getModelName("metric", model, packageName!!)
                }
            }
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
