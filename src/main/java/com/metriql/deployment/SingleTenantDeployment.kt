package com.metriql.deployment

import com.fasterxml.jackson.core.type.TypeReference
import com.google.common.base.Splitter
import com.google.common.cache.CacheBuilderSpec
import com.metriql.Commands
import com.metriql.UserContext
import com.metriql.dbt.DbtJinjaRenderer
import com.metriql.dbt.DbtManifestParser
import com.metriql.dbt.DbtProfiles
import com.metriql.dbt.DbtYamlParser
import com.metriql.dbt.ProjectYaml
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.auth.ProjectAuth.Companion.PASSWORD_CREDENTIAL
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Dataset
import com.metriql.service.model.UpdatableDatasetService
import com.metriql.service.suggestion.InMemorySuggestionCacheService
import com.metriql.service.suggestion.SuggestionCacheService
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.RecipeUtil
import com.metriql.util.UnirestHelper
import com.metriql.util.YamlHelper
import com.metriql.warehouse.WarehouseConfig
import com.metriql.warehouse.WarehouseLocator
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import io.netty.handler.codec.http.HttpResponseStatus
import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.ZoneId

class SingleTenantDeployment(
    private val manifestJson: String,
    private val modelsFilter: String?,
    private val passCredentialsToDatasource: Boolean,
    private val timezone: ZoneId?,
    usernamePass: String?,
    projectDir: String,
    profilesContent: String?,
    profilesDir: String,
    vars: String?,
    profile: String?,
    private val cacheSpec: CacheBuilderSpec,
) : Deployment {
    private val profileConfig = getProfileConfigForSingleTenant(projectDir, profilesContent, profilesDir, vars, profile)
    private val singleAuth = ProjectAuth.singleProject()
    private val datasetService = UpdatableDatasetService(null) {
        val dataSource = getDataSource(singleAuth)
        getPreparedModels(dataSource, singleAuth, parseRecipe(dataSource, manifestJson, modelsFilter))
    }
    private val usernamePassPair = usernamePass?.let { Commands.parseUserNamePass(it) }
    override val authType = if (usernamePassPair == null) Deployment.AuthType.NONE else Deployment.AuthType.USERNAME_PASS

    override fun getAuth(it: UserContext): ProjectAuth {
        val validCredentials = usernamePassPair != null && it.user == usernamePassPair.first && it.pass == usernamePassPair.second
        return if (validCredentials || usernamePassPair == null) {
            return ProjectAuth(
                it.user ?: "(unknown user)", "",
                isOwner = true, isSuperuser = true, email = null, permissions = null, attributes = mapOf(),
                timezone = timezone, source = null, credentials = it.pass?.let { pass -> mapOf(PASSWORD_CREDENTIAL to pass) }
            )
        } else throw MetriqlException(HttpResponseStatus.UNAUTHORIZED)
    }

    override fun getDatasetService() = datasetService

    override fun getCacheService(): SuggestionCacheService {
        return InMemorySuggestionCacheService(cacheSpec)
    }

    override fun logStart() {
        Commands.logger.info("Serving ${datasetService.list(singleAuth).size} datasets")
    }

    override fun getDataSource(auth: ProjectAuth): DataSource {
        val config = if (passCredentialsToDatasource && auth != singleAuth) {
            profileConfig.copy(value = profileConfig.value.withUsernamePassword(auth.userId.toString(), auth.credentials?.get(PASSWORD_CREDENTIAL).toString()))
        } else profileConfig

        return WarehouseLocator.getDataSource(config)
    }

    companion object {
        fun getProfileConfigForSingleTenant(projectDir: String, profilesContent: String?, profilesDir: String?, vars: String?, profile: String?): WarehouseConfig {
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
                    throw IllegalArgumentException("profiles.yml does not exist in ${profilesFile.absoluteFile}. Please set --profiles-dir option.")
                }
                profilesFile.readText(StandardCharsets.UTF_8)
            }

            val varMap = if (vars != null) {
                YamlHelper.mapper.readValue(vars, object : TypeReference<Map<String, Any?>>() {})
            } else mapOf()

            val compiledProfiles = DbtJinjaRenderer.renderer.renderProfiles(content, varMap)

            val profiles = YamlHelper.mapper.readValue(compiledProfiles, DbtProfiles::class.java)
            val profileName = profile ?: dbtProjectFile?.profile ?: "default"
            val currentProfile =
                profiles[profileName] ?: throw IllegalStateException("Profile `$profileName` doesn't exist, available profiles are ${profiles.keys.joinToString(", ")}")

            return JsonHelper.convert(currentProfile.outputs[currentProfile.target], WarehouseConfig::class.java)
        }

        private fun resolveExtends(allModels: List<Recipe.RecipeModel>, it: Recipe.RecipeModel, packageName: String): Recipe.RecipeModel {
            return if (it.extends != null) {
                val ref = DbtJinjaRenderer.renderer.renderModelNameRegex(it.extends).toRegex()
                val parentModel = allModels.find { model -> ref.matches(model.name!!) } ?: throw MetriqlException(
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

        fun getPreparedModels(dataSource: DataSource, auth: ProjectAuth, recipe: Recipe): List<Dataset> {
            val metriqlModels = recipe.models?.map {
                resolveExtends(recipe.models, it, recipe.packageName ?: "").toModel(recipe.packageName ?: "", dataSource.warehouse.bridge)
            } ?: listOf()
            val context = QueryGeneratorContext(auth, dataSource, UpdatableDatasetService(null) { metriqlModels }, JinjaRendererService(), null, null, null)
            return RecipeUtil.prepareModelsForInstallation(dataSource, context, metriqlModels)
        }

        fun parseRecipe(dataSource: DataSource, manifestJson: String, modelsFilter: String? = null, packageName: String = "(inline)"): Recipe {
            val manifestLocation = URI(manifestJson)
            val content = when (manifestLocation.scheme) {
                "http", "https" -> {
                    val request = UnirestHelper.unirest.get(manifestJson)
                    if (manifestLocation.userInfo != null) {
                        val (user, pass) = Commands.parseUserNamePass(manifestLocation.userInfo)
                        request.basicAuth(user, pass)
                    }

                    val response = request.asBytes()
                    if (response.status != 200) {
                        throw IllegalArgumentException(
                            "Unable to fetch manifest file from $manifestJson: ${response.statusText}",
                        )
                    } else response.body
                }
                "dbt-cloud" -> getDbtCloud(manifestLocation)
                "file" -> {
                    val file = File(manifestLocation).absoluteFile
                    if (!file.exists()) {
                        throw IllegalArgumentException(
                            "manifest.json file (specified in --manifest-json option) could not found, please compile dbt models before running Metriql, path is: $file",
                        )
                    } else {
                        file.readBytes()
                    }
                }
                null -> {
                    throw IllegalArgumentException("Manifest file should be an URI with one of http, https, and file schema. Example: file:/etc/manifest.json")
                }
                else -> {
                    throw IllegalArgumentException("Manifest file scheme ${manifestLocation.scheme} is not supported. $manifestJson")
                }
            }

            val models = try {
                DbtManifestParser.parse(dataSource, content, modelsFilter)
            } catch (manifestEx: DbtYamlParser.ParseException) {
                // support both dbt and metriql manifest file in the same config but throw dbt exception as it's the default method
                try {
                    JsonHelper.read(content, object : TypeReference<List<Dataset>>() {}).map { Recipe.RecipeModel.fromModel(it) }
                } catch (metriqlModelEx: Exception) {
                    throw manifestEx
                }
            }

            return Recipe("local://metriql", "master", null, Recipe.Config(packageName), packageName, models = models)
        }

        private fun getDbtCloud(manifestLocation: URI): ByteArray {
            val project = try {
                Integer.parseInt((manifestLocation.path ?: "/").substring(1))
            } catch (e: Exception) {
                throw IllegalArgumentException("Unable to parse the project for dbt-cloud scheme. $DBT_CLOUD_URL")
            }

            val query: Map<String, String> = Splitter.on('&').trimResults()
                .withKeyValueSeparator('=').split(manifestLocation.query)

            val jobId = query["job_id"]?.get(0) ?: throw IllegalArgumentException("{job_id} query parameter is missing in dbt-cloud URI. $DBT_CLOUD_URL")
            if (manifestLocation.userInfo == null) {
                throw IllegalArgumentException("{api_key} is missing in dbt-cloud URI. $DBT_CLOUD_URL")
            }
            val lastRunRequest = UnirestHelper.unirest
                .get("https://${manifestLocation.host}/api/v2/accounts/$project/runs?job_definition_id=$jobId&limit=1&order_by=-finished_at")
                .header("Authorization", "Token ${manifestLocation.userInfo}")
                .asJson()
            if (lastRunRequest.status != 200) {
                throw IllegalArgumentException("Unable to fetch last run id from dbt Cloud: ${lastRunRequest.body}")
            }
            var runId: String? = lastRunRequest.body.`object`.getJSONArray("data")?.getJSONObject(0)?.getString("id")
                ?: throw IllegalArgumentException("Unable to fetch last run id from dbt Cloud, there should be at least one successful run for job id: $jobId")

            val manifestFileRequest = UnirestHelper.unirest
                .get("https://${manifestLocation.host}/api/v2/accounts/$project/runs/$runId/artifacts/manifest.json")
                .header("Authorization", "Token ${manifestLocation.userInfo}")
                .asBytes()

            if (manifestFileRequest.status != 200) {
                throw IllegalArgumentException("Unable to manifest.json file from run id $runId: ${String(manifestFileRequest.body)}")
            }

            return manifestFileRequest.body
        }

        private const val DBT_CLOUD_URL = "It should follow the following format: dbt-cloud://{api_key}@{dbt_cloud_url}/{account_id}?job_id={job_id}"
    }
}
