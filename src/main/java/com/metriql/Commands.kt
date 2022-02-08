package com.metriql

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.output.CliktHelpFormatter
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.defaultLazy
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.google.common.net.HostAndPort
import com.metriql.dbt.DbtModelService
import com.metriql.dbt.FileHandler
import com.metriql.deployment.Deployment
import com.metriql.deployment.MultiTenantDeployment
import com.metriql.deployment.SingleTenantDeployment
import com.metriql.deployment.SingleTenantDeployment.Companion.getProfileConfigForSingleTenant
import com.metriql.deployment.SingleTenantDeployment.Companion.parseRecipe
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.data.recipe.Recipe.Dependencies.DbtDependency
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.ModelName
import com.metriql.util.JsonHelper
import com.metriql.util.TextUtil
import com.metriql.warehouse.WarehouseLocator
import com.metriql.warehouse.metriql.CatalogFile
import com.metriql.warehouse.spi.querycontext.DependencyFetcher
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import io.airlift.units.Duration
import java.io.File
import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import kotlin.system.exitProcess

open class Commands(help: String? = null) : CliktCommand(help = help ?: "", printHelpOnEmptyArgs = help == null) {
    val version by option("--version", help = "Print version name and exit", hidden = true).flag()
    val debug by option("-d", "--debug", help = "Enable debugging").flag()
    val profilesDir by option(
        "--profiles-dir",
        help = "Which directory to look in for the profiles.yml file. Default = ~/.dbt",
        envvar = "DBT_PROFILES_DIR"
    ).defaultLazy { "${System.getProperty("user.home")}/.dbt/" }
    protected val profilesContent by option(
        help = "Profiles content as YML, overrides --profiles-dir option",
        envvar = "DBT_PROFILES_CONTENT"
    )
    val profile by option("--profile", help = "Which profile to load. Overrides setting in dbt_project.yml.", envvar = "PROFILE")
    val models by option("--models", help = "Which models to expose as datasets", envvar = "METRIQL_MODELS")

    val projectDir by option(
        "--project-dir",
        help = "Which directory to look in for the dbt_project.yml file. Default is the current working directory and its parents.",
        envvar = "DBT_PROJECT_DIR"
    ).defaultLazy { File("").absoluteFile.toURI().toString() }
    val vars by option(
        "--vars",
        envvar = "METRIQL_VARS",
        help = "Supply variables to the project. " +
            "This argument overrides variables defined in your dbt_project.yml file. " +
            "This argument should be a YAML string, eg. '{my_variable: my_value}'"
    )
    val multiTenantUrl by option(
        "--multi-tenant-url",
        help = "Enables multi-tenant deployment using the auth URL that you provided. Ignores all the other parameters.",
        envvar = "METRIQL_MULTI_TENANT_URL"
    )
    val multiTenantCacheDuration by option(
        "--multi-tenant-cache-duration",
        help = "The cache duration for successful auth requests in when multi-tenant deployment is enabled. You can use `m` for minutes, `s` for seconds, and `h` for hours.",
        envvar = "METRIQL_MULTI_TENANT_CACHE_DURATION"
    ).default("10m")

    init {
        context { helpFormatter = CliktHelpFormatter(showDefaultValues = true) }
    }

    override fun aliases(): Map<String, List<String>> = mapOf(
        "run" to listOf("serve")
    )

    override fun run() {
    }

    class Test : Commands(help = "Tests metriql datasets with metadata queries") {
        override fun run() {
        }
    }

    class Generate : Commands(help = "Generates materialized dbt models for aggregates") {
        private val outputDir by option("--output-dir", "-o", help = "Which directory to create aggregate models.", envvar = "METRIQL_OUTPUT_DIR").default("models/rakam")

        override fun run() {
            super.checkVersion()
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

            val auth = ProjectAuth.singleProject()
            val config = try {
                getProfileConfigForSingleTenant(projectDir, super.profilesContent, profilesDir, vars, profile)
            } catch (e: IllegalArgumentException) {
                echo(e.message, err = true)
                exitProcess(1)
            }

            val dataSource = WarehouseLocator.getDataSource(config)

            val successfulCounts = AtomicInteger()

            val dependencies = Recipe.Dependencies(DbtDependency(aggregatesDirectory = outputDir))
            val recipe = try {
                val manifestJson = manifestFile.toURI().toString()
                echo("Fetching manifest.json file from $manifestJson")
                parseRecipe(dataSource, manifestJson, models).copy(dependencies = dependencies)
            } catch (e: IllegalArgumentException) {
                echo(e.message, err = true)
                exitProcess(1)
            }
            val service = DbtModelService(
                JinjaRendererService(), null,
                object : DependencyFetcher {
                    override fun fetch(context: IQueryGeneratorContext, model: ModelName): Recipe.Dependencies {
                        return dependencies
                    }
                }
            )

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

    class Serve(val deployment : Deployment? = null) : Commands(help = "Spins up an HTTP server serving your datasets") {
        private val origin by option("--origin", help = "The origin HTTP server for CORS", envvar = "METRIQL_ORIGIN")
        private val enableTrinoInterface by option("--trino", "--jdbc", help = "Enable Trino API", envvar = "METRIQL_ENABLE_JDBC").flag(default = true)
        private val threads by option("--threads", help = "Specify number of threads to use serving requests. The default is [number of processors * 2]", envvar = "THREADS").int()
            .defaultLazy { Runtime.getRuntime().availableProcessors() * 2 }
        val port by option("--port", envvar = "PORT", help = "").int().default(5656)
        val host by option("--host", "-h", envvar = "HOST", help = "The binding host for the REST API").default("127.0.0.1")
        val timezone by option("--timezone", envvar = "METRIQL_TIMEZONE", help = "The timezone that will be used running queries on your data warehouse")
        private val apiSecretBase64 by option(
            "--api-auth-secret-base64", envvar = "METRIQL_API_AUTH_SECRET_BASE64",
            help = "Your JWT secret key in Base64 format. metriql supports various algorithms such as HS256 and RS256 and identifies the key parsing the content."
        )
        private val usernamePass by option(
            "--api-auth-username-password", envvar = "METRIQL_API_AUTH_USERNAME_PASSWORD",
            help = "Your username:password pair for basic authentication"
        )
        private val passCredentialsToDatasource by option(
            "--pass-credentials-to-datasource", envvar = "METRIQL_API_PASS_CREDENTIALS_TO_DATASOURCE",
            help = "Pass username & password to datasource configs"
        ).flag(default = false)
        private val catalogFile by option(
            "--catalog-file", envvar = "METRIQL_CATALOG_FILE",
            help = "Metriql catalog file"
        )
        private val apiSecretFile by option(
            "--api-auth-secret-file", envvar = "METRIQL_API_AUTH_SECRET_FILE",
            help = "If you're using metriql locally, you can set the private key file or API secret as a file argument."
        )
        private val manifestJson by option(
            "--manifest-json",
            help = "The URI of the manifest.json, `file`, `http`, and `https` is supported. The default is \$DBT_PROJECT_DIR/target/manifest.json",
            envvar = "DBT_MANIFEST_JSON"
        )

        override fun run() {
            super.checkVersion()
            val apiSecret = when {
                apiSecretBase64 != null -> {
                    String(Base64.getDecoder().decode(apiSecretBase64), StandardCharsets.UTF_8)
                }
                apiSecretFile != null -> {
                    File(apiSecretFile).readText(StandardCharsets.UTF_8)
                }
                else -> null
            }

            val timezone = timezone?.let { ZoneId.of(it) }

            val arg = manifestJson ?: File(projectDir, "target/manifest.json").toURI().toString()
            val deployment = deployment ?: if (multiTenantUrl != null) {
                MultiTenantDeployment(multiTenantUrl!!, Duration.valueOf(multiTenantCacheDuration))
            } else {
                SingleTenantDeployment(arg, models, passCredentialsToDatasource, timezone, usernamePass, projectDir, super.profilesContent, profilesDir, vars, profile)
            }

            val catalogFile = when {
                catalogFile != null -> JsonHelper.read(FileInputStream(catalogFile), CatalogFile::class.java)
                else -> null
            }

            val httpPort = System.getenv("METRIQL_RUN_PORT")?.let { Integer.parseInt(it) } ?: port
            val httpHost = System.getenv("METRIQL_RUN_HOST") ?: host
            HttpServer.start(
                HostAndPort.fromParts(httpHost, httpPort), apiSecret, threads, debug, origin,
                deployment, enableTrinoInterface, timezone, catalogFile?.catalogs
            )
        }
    }

    protected fun checkVersion() {
        if (version) {
            echo(TextUtil.version(), trailingNewline = true)
            exitProcess(0)
        }
    }

    companion object {
        val logger: Logger = Logger.getLogger(this::class.java.name)

        fun parseUserNamePass(usernamePass: String): Pair<String, String> {
            val arr = usernamePass.split(":".toRegex(), 2)
            if (arr.size != 2) {
                throw IllegalArgumentException("Invalid argument for user pass: $usernamePass")
            }
            return Pair(arr[0], arr[1])
        }
    }
}
