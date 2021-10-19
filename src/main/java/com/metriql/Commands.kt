package com.metriql

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.defaultLazy
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.google.common.base.Splitter
import com.google.common.net.HostAndPort
import com.metriql.dbt.DbtJinjaRenderer
import com.metriql.dbt.DbtManifestParser
import com.metriql.dbt.DbtModelConverter
import com.metriql.dbt.DbtModelService
import com.metriql.dbt.DbtProfiles
import com.metriql.dbt.FileHandler
import com.metriql.dbt.ProjectYaml
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.data.recipe.Recipe.Dependencies.DbtDependency
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.IModelService
import com.metriql.service.model.Model
import com.metriql.service.model.ModelName
import com.metriql.service.model.UpdatableModelService
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.RecipeUtil.prepareModelsForInstallation
import com.metriql.util.TextUtil
import com.metriql.util.UnirestHelper
import com.metriql.util.YamlHelper
import com.metriql.warehouse.WarehouseConfig
import com.metriql.warehouse.WarehouseLocator
import com.metriql.warehouse.metriql.CatalogFile
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.DependencyFetcher
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import java.io.File
import java.io.FileInputStream
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
    private val profilesContent by option(
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

    override fun aliases(): Map<String, List<String>> = mapOf(
        "run" to listOf("serve")
    )

    override fun run() {
    }

    protected fun getProfileConfig(): WarehouseConfig {
        val dbtProjectFile = File(projectDir, "dbt_project.yml")?.let {
            if (it.exists()) {
                YamlHelper.mapper.readValue(it.readBytes(), ProjectYaml::class.java)
            } else null
        }

        val content = if (profilesContent != null) {
            profilesContent!!
        } else {
            val profilesFile = File(profilesDir, "profiles.yml")
            if (!profilesFile.exists()) {
                echo("profiles.yml does not exist in ${profilesFile.absoluteFile}. Please set --profiles-dir option.", err = true)
                exitProcess(1)
            }
            profilesFile.readText(StandardCharsets.UTF_8)
        }

        val varMap = if (vars != null) {
            YamlHelper.mapper.readValue(vars, object : TypeReference<Map<String, Any?>>() {})
        } else mapOf()

        val compiledProfiles = DbtJinjaRenderer.renderer.renderProfiles(content, varMap)

        val profiles = YamlHelper.mapper.readValue(compiledProfiles, DbtProfiles::class.java)
        val currentProfile = profiles[profile ?: dbtProjectFile?.profile ?: "default"]
        if (currentProfile == null) {
            echo("profile $profile doesn't exist", err = true)
            exitProcess(1)
            // it's not reachable anyway
            return null!!
        }

        return JsonHelper.convert(currentProfile.outputs[currentProfile.target], WarehouseConfig::class.java)
    }

    protected fun parseRecipe(dataSource: DataSource, manifestJson: String, packageName: String = "(inline)"): Recipe {
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
            "dbt-cloud" -> getDbtCloud(manifestLocation)
            "file" -> {
                val file = File(manifestLocation).absoluteFile
                if (!file.exists()) {
                    echo(
                        "manifest.json file (specified in --manifest-json option) could not found, please compile dbt models before running metriql, path is: $file",
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
                DbtManifestParser.parse(dataSource, content, models)
            } catch (manifestEx: MismatchedInputException) {
                // support both dbt and metriql manifest file in the same config but throw dbt exception as it's the default method
                try {
                    JsonHelper.read(content, object : TypeReference<List<Model>>() {}).map { Recipe.RecipeModel.fromModel(it) }
                } catch (metriqlModelEx: Exception) {
                    throw manifestEx
                }
            }

            Recipe("local://metriql", "master", null, Recipe.Config(packageName), packageName, models = models)
        }
    }

    private fun getDbtCloud(manifestLocation: URI): ByteArray? {
        val project = try {
            Integer.parseInt((manifestLocation.path ?: "/").substring(1))
        } catch (e: Exception) {
            echo("Unable to parse the project for dbt-cloud scheme. $DBT_CLOUD_URL", err = true)
            return null
        }

        val query: Map<String, String> = Splitter.on('&').trimResults()
            .withKeyValueSeparator('=').split(manifestLocation.query)

        val jobId = query["job_id"]?.get(0)
        if (jobId == null) {
            echo("{job_id} query parameter is missing in dbt-cloud URI. $DBT_CLOUD_URL", err = true)
            return null
        }
        if (manifestLocation.userInfo == null) {
            echo("{api_key} is missing in dbt-cloud URI. $DBT_CLOUD_URL", err = true)
            return null
        }
        val lastRunRequest = UnirestHelper.unirest
            .get("https://${manifestLocation.host}/api/v2/accounts/$project/runs?job_definition_id=$jobId&limit=1&order_by=-finished_at")
            .header("Authorization", "Token ${manifestLocation.userInfo}")
            .asJson()
        if (lastRunRequest.status != 200) {
            echo("Unable to fetch last run id from dbt Cloud: ${lastRunRequest.body}")
            return null
        }
        var runId = lastRunRequest.body.`object`.getJSONArray("data")?.getJSONObject(0)?.getString("id")
        if (runId == null) {
            echo("Unable to fetch last run id from dbt Cloud, there should be at least one successful run for job id: $jobId")
            return null
        }

        val manifestFileRequest = UnirestHelper.unirest
            .get("https://${manifestLocation.host}/api/v2/accounts/$project/runs/$runId/artifacts/manifest.json")
            .header("Authorization", "Token ${manifestLocation.userInfo}")
            .asBytes()

        if (manifestFileRequest.status != 200) {
            echo("Unable to manifest.json file from run id $runId: ${String(manifestFileRequest.body)}")
            return null
        }

        return manifestFileRequest.body
    }

    class Test : Commands(help = "Tests metriql datasets with metadata queries") {
        override fun run() {
        }
    }

    class Generate : Commands(help = "Generates materialized dbt models for aggregates") {
        private val outputDir by option("--output-dir", "-o", help = "Which directory to create aggregate models.", envvar = "METRIQL_OUTPUT_DIR").default("models/rakam")

        override fun run() {
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
            val dataSource = WarehouseLocator.getDataSource(this.getProfileConfig())

            val successfulCounts = AtomicInteger()

            val dependencies = Recipe.Dependencies(DbtDependency(aggregatesDirectory = outputDir))
            val recipe = parseRecipe(dataSource, manifestFile.toURI().toString()).copy(dependencies = dependencies)
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

    class Serve : Commands(help = "Spins up an HTTP server serving your datasets") {
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
            val apiSecret = when {
                apiSecretBase64 != null -> {
                    String(Base64.getDecoder().decode(apiSecretBase64), StandardCharsets.UTF_8)
                }
                apiSecretFile != null -> {
                    File(apiSecretFile).readText(StandardCharsets.UTF_8)
                }
                else -> null
            }

            val deployment = CommunityDeployment()

            val catalogFile = when {
                catalogFile != null -> JsonHelper.read(FileInputStream(catalogFile), CatalogFile::class.java)
                else -> null
            }

            val httpPort = System.getenv("METRIQL_RUN_PORT")?.let { Integer.parseInt(it) } ?: port
            val httpHost = System.getenv("METRIQL_RUN_HOST") ?: host
            HttpServer.start(
                HostAndPort.fromParts(httpHost, httpPort), apiSecret, usernamePass, threads, debug, origin,
                deployment, enableTrinoInterface, timezone?.let { ZoneId.of(it) }, catalogFile?.catalogs
            )
        }

        private fun resolveExtends(allModels: List<Recipe.RecipeModel>, it: Recipe.RecipeModel): Recipe.RecipeModel {
            return if (it.extends != null) {
                val ref = DbtModelConverter.parseRef(it.extends)
                val parentModel = allModels.find { model -> model.name == ref } ?: throw MetriqlException(
                    "${it.name}: extends ${it.extends} not found.",
                    HttpResponseStatus.BAD_REQUEST
                )
                it.copy(
                    dimensions = (it.dimensions ?: mapOf()) + (parentModel.dimensions ?: mapOf()),
                    measures = (it.measures ?: mapOf()) + (parentModel.measures ?: mapOf()),
                    relations = (it.relations ?: mapOf()) + (parentModel.relations ?: mapOf())
                )
            } else it
        }

        inner class EnterpriseDeployment : CommunityDeployment() {

        }

        open inner class CommunityDeployment : Deployment {
            private val profileConfig = getProfileConfig()
            private val singleAuth = ProjectAuth.singleProject()
            private val modelService = UpdatableModelService(null) { getModels(singleAuth) }

            private fun getModels(auth: ProjectAuth): List<Model> {
                val dataSource = getDataSource(auth)
                val manifest = manifestJson ?: File(projectDir, "target/manifest.json").toURI().toString()
                val recipe = parseRecipe(dataSource, manifest)
                val metriqlModels = recipe.models?.map {
                    resolveExtends(recipe.models, it).toModel(recipe.packageName ?: "", dataSource.warehouse.bridge, -1)
                } ?: listOf()
                val context = QueryGeneratorContext(ProjectAuth.systemUser(-1), dataSource, UpdatableModelService(null) { metriqlModels }, JinjaRendererService(), null, null, null)
                return prepareModelsForInstallation(dataSource, context, metriqlModels)
            }

            override fun getModelService() = modelService

            override fun logStart() {
                logger.info("Serving ${modelService.list(singleAuth).size} datasets")
            }

            override fun getDataSource(auth: ProjectAuth): DataSource {
                val config = if (passCredentialsToDatasource) {
                    profileConfig.value.withUsernamePassword(null!!, null!!)
                } else profileConfig

                return WarehouseLocator.getDataSource(profileConfig)
            }
        }
    }

    companion object {
        internal val logger = Logger.getLogger(this::class.java.name)

        const val DBT_CLOUD_URL = "It should follow the following format: dbt-cloud://{api_key}@{dbt_cloud_url}/{account_id}?job_id={job_id}"

        fun parseUserNamePass(usernamePass: String): Pair<String, String> {
            val arr = usernamePass.split(":".toRegex(), 2)
            if (arr.size != 2) {
                throw IllegalArgumentException("Invalid argument for user pass: $usernamePass")
            }
            return Pair(arr[0], arr[1])
        }
    }

    interface Deployment {
        fun getModelService(): IModelService
        fun logStart()
        fun getDataSource(auth: ProjectAuth): DataSource
    }
}
