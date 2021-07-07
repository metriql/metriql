package com.metriql

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.defaultLazy
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.google.common.net.HostAndPort
import com.metriql.dbt.DbtManifestParser
import com.metriql.dbt.DbtModelService
import com.metriql.dbt.DbtProfiles
import com.metriql.dbt.FileHandler
import com.metriql.dbt.ProjectYaml
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.data.recipe.Recipe.Dependencies.DbtDependency
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Model
import com.metriql.service.model.RecipeModelService
import com.metriql.util.JsonHelper
import com.metriql.util.TextUtil
import com.metriql.util.UnirestHelper
import com.metriql.util.YamlHelper
import com.metriql.warehouse.WarehouseConfig
import com.metriql.warehouse.WarehouseLocator
import com.metriql.warehouse.spi.DataSource
import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import kotlin.system.exitProcess

open class Commands(help: String? = null) : CliktCommand(help = help ?: "", printHelpOnEmptyArgs = help == null) {
    val version by option("--version", help = "Print version name and exit", hidden = true).flag()
    val debug by option("-d", "--debug", help = "Enable debugging").flag()
    private val profilesDir by option(
        "--profiles-dir",
        help = "Which directory to look in for the profiles.yml file. Default = ~/.dbt",
        envvar = "DBT_PROFILES_DIR"
    ).defaultLazy { "${System.getProperty("user.home")}/.dbt/" }
    val profile by option("--profile", help = "Which profile to load. Overrides setting in dbt_project.yml.", envvar = "PROFILE")

    val projectDir by option(
        "--project-dir",
        help = "Which directory to look in for the dbt_project.yml file. Default is the current working directory and its parents.",
        envvar = "DBT_PROJECT_DIR"
    ).defaultLazy { File("").toURI().toString() }

    override fun run() {
    }

    protected fun getDataSource(): DataSource {
        val dbtProjectFile = File(projectDir, "dbt_project.yml")?.let {
            if (it.exists()) {
                YamlHelper.mapper.readValue(it.readBytes(), ProjectYaml::class.java)
            } else null
        }

        val profilesFile = File(profilesDir, "profiles.yml")
        if (!profilesFile.exists()) {
            echo("profiles.yml does not exist in ${profilesFile.absoluteFile}. Please set --profiles-dir option.", err = true)
            exitProcess(1)
        }
        val profiles = YamlHelper.mapper.readValue(profilesFile.readBytes(), DbtProfiles::class.java)
        val currentProfile = profiles[profile ?: dbtProjectFile?.profile ?: "default"]
        if (currentProfile == null) {
            echo("profile $profile doesn't exist", err = true)
            exitProcess(1)
            // it's not reachable anyway
            return null!!
        }

        val config = JsonHelper.convert(currentProfile.outputs[currentProfile.target], WarehouseConfig::class.java)
        return WarehouseLocator.getDataSource(config)
    }

    protected fun parseRecipe(manifestJson: String): Recipe {
        if (version) {
            echo(TextUtil.version(), trailingNewline = true)
            exitProcess(0)
        }

        val manifestLocation = URI(manifestJson)
        val content = when (manifestLocation.scheme) {
            "http", "https" -> {
                val request = UnirestHelper.unirest.get(manifestJson)
                if (manifestLocation.userInfo != null) {
                    val (user, pass) = parseUserNamePass(manifestLocation.userInfo)
                    request.basicAuth(user, pass)
                }
                echo("Fetching manifest.json file from $manifestJson")

                val response = request.asBytes()
                if (response.status != 200) {
                    echo(
                        "Unable to fetch manifest file from $manifestJson: ${response.statusText}",
                        err = true
                    )
                    null
                } else response.body
            }
            "file" -> {
                val file = File(manifestLocation).absoluteFile
                if (!file.exists()) {
                    echo(
                        "manifest.json file (specified in --manifest-json option) could not found, please compile dbt models before running metriql, current uri is: $manifestJson",
                        err = true
                    )
                    null
                } else {
                    file.readBytes()
                }
            }
            null -> {
                echo("Manifest file should be an URI with one of http, https, and file schema. Example: file:/etc/manifest.json")
                null
            }
            else -> {
                echo("Manifest file scheme ${manifestLocation.scheme} is not supported. $manifestJson", err = true)
                null
            }
        }

        return if (content == null) {
            exitProcess(1)
            // this code is unreachable as we're exiting the process
            null!!
        } else {
            val models = try {
                DbtManifestParser.parse(content)
            } catch (manifestEx: MismatchedInputException) {
                // support both dbt and metriql manifest file in the same config but throw dbt exception as it's the default method
                try {
                    JsonHelper.read(content, object : TypeReference<List<Model>>() {}).map { Recipe.RecipeModel.fromModel(it) }
                } catch (metriqlModelEx: Exception) {
                    throw manifestEx
                }
            }
            Recipe("local://metriql", "master", null, Recipe.Config("(inline)"), "(inline)", models = models)
        }
    }

    class Generate : Commands(help = "Generates dbt models for aggregates") {
        private val outputDir by option("--output-dir", "-o", help = "Which directory to create aggregate models.", envvar = "METRIQL_OUTPUT_DIR").default("models/rakam")

        override fun run() {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%4\$s: %5\$s%n")

            val dbtProject = File(projectDir, "dbt_project.yml")
            if (!dbtProject.exists()) {
                echo("dbt_project.yml doesn't exist in `${dbtProject.absolutePath}`, not a valid dbt project.", err = true)
                exitProcess(1)
            }

            val manifestFile = File(projectDir, "target/manifest.json")
            if (!manifestFile.exists()) {
                echo("manifest.json doesn't exist in `${manifestFile.absolutePath}`, please run `dbt list` or `dbt compile` before running metriql.", err = true)
                exitProcess(1)
            }

            val service = DbtModelService(JinjaRendererService(), null)
            val auth = ProjectAuth.singleProject()
            val dataSource = this.getDataSource()

            val successfulCounts = AtomicInteger()

            val recipe = parseRecipe(manifestFile.toURI().toString()).copy(dependencies = Recipe.Dependencies(DbtDependency(aggregatesDirectory = outputDir)))
            val errors = service.addDbtFiles(
                auth,
                object : FileHandler {
                    override fun addFile(path: String, content: String) {
                        echo("Creating dbt model $path")
                        successfulCounts.incrementAndGet()
                        val file = File(projectDir, path)
                        val parentFile = file.parentFile
                        if (!parentFile.exists()) {
                            parentFile.mkdirs()
                        }
                        file.writeBytes(content.toByteArray(StandardCharsets.UTF_8))
                    }

                    override fun deletePath(path: String) {
                        echo("Deleting directory $path")
                        File(dbtProject, path).deleteRecursively()
                    }

                    override fun deleteFile(path: String) = throw IllegalStateException()
                    override fun commit(message: String) = throw IllegalStateException()
                    override fun getContent() = throw IllegalStateException()
                    override fun cancel() = throw IllegalStateException()
                },
                recipe, dataSource, -1
            )

            errors.forEach {
                logger.severe(it.title)
            }

            if (errors.isNotEmpty() && successfulCounts.get() == 0) {
                echo("Failed creating ${errors.size} models", err = true)
            } else {
                val errorMessage = if (errors.isNotEmpty()) " with ${errors.size} failures" else ""
                echo("Done creating ${successfulCounts.get()} aggregate dbt models$errorMessage.")
            }
        }
    }

    class Run : Commands(help = "Spins up an HTTP server serving your datasets") {
        private val origin by option("--origin", help = "The origin HTTP server for CORS", envvar = "METRIQL_ORIGIN")
        private val enableJdbc by option("--jdbc", help = "Enable JDBC services via Trino Proxy", envvar = "METRIQL_ENABLE_JDBC").flag()
        val vars by option(
            "--vars",
            envvar = "METRIQL_VARS",
            help = "Supply variables to the project. " +
                "This argument overrides variables defined in your dbt_project.yml file. " +
                "This argument should be a YAML string, eg. '{my_variable: my_value}'"
        )
        private val threads by option("--threads", help = "Specify number of threads to use serving requests. The default is [number of processors * 2]", envvar = "THREADS").int()
            .defaultLazy { Runtime.getRuntime().availableProcessors() * 2 }
        val port by option("--port", envvar = "METRIQL_RUN_PORT", help = "").int().default(5656)
        val host by option("--host", "-h", envvar = "METRIQL_RUN_HOST", help = "The binding host for the REST API").default("127.0.0.1")
        val timezone by option("--timezone", envvar = "METRIQL_TIMEZONE", help = "The timezone that will be used running queries on your data warehouse")
        private val apiSecretBase64 by option(
            "--api-auth-secret-base64", envvar = "METRIQL_API_AUTH_SECRET_BASE64",
            help = "Your JWT secret key in Base64 format. metriql supports various algorithms such as HS256 and RS256 and identifies the key parsing the content."
        )
        private val usernamePass by option(
            "--api-auth-username-password", envvar = "METRIQL_API_AUTH_USERNAME_PASSWORD",
            help = "Your username:password pair for basic authentication"
        )
        private val apiSecretFile by option(
            "--api-auth-secret-file", envvar = "METRIQL_API_AUTH_SECRET_FILE",
            help = "If you're using metriql locally, you can set the private key file or API secret as a file argument."
        )
        private val manifestJson by option(
            "--manifest-json",
            help = "The URI of the manifest.json, `file`, `http`, and `https` is supported",
            envvar = "DBT_MANIFEST_JSON"
        ).defaultLazy { File("target/manifest.json").toURI().toString() }

        override fun run() {
            val apiSecret = when {
                apiSecretBase64 != null -> {
                    String(Base64.getDecoder().decode(apiSecretBase64), StandardCharsets.UTF_8)
                }
                apiSecretFile != null -> {
                    File(apiSecretFile).readText(StandardCharsets.UTF_8)
                }
                else -> null
            }

            val dataSource = this.getDataSource()

            val modelService = RecipeModelService(null, { this.parseRecipe(manifestJson) }, -1, dataSource.warehouse.bridge)
            HttpServer.start(
                HostAndPort.fromParts(host, port), apiSecret, usernamePass, threads, debug, origin,
                modelService, dataSource, enableJdbc, timezone?.let { ZoneId.of(it) }
            )
        }
    }

    companion object {
        fun parseUserNamePass(usernamePass: String): Pair<String, String> {
            val arr = usernamePass.split(":".toRegex(), 2)
            if (arr.size != 2) {
                throw IllegalArgumentException("Invalid argument for user pass: $usernamePass")
            }
            return Pair(arr[0], arr[1])
        }
    }

    internal val logger = Logger.getLogger(this::class.java.name)
}
