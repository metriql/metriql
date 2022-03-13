package com.metriql.dbt

import com.hubspot.jinjava.Jinjava
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.hubspot.jinjava.lib.filter.Filter
import com.metriql.db.FieldType
import com.metriql.report.ReportType
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.data.recipe.Recipe.Dependencies.DbtDependency
import com.metriql.report.segmentation.SegmentationService
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.IDatasetService
import com.metriql.service.model.Model.MappingDimensions.CommonMappings.EVENT_TIMESTAMP
import com.metriql.service.model.UpdatableDatasetService
import com.metriql.util.JsonHelper
import com.metriql.util.MetriqlException
import com.metriql.util.YamlHelper
import com.metriql.warehouse.spi.DataSource
import com.metriql.warehouse.spi.bridge.WarehouseMetriqlBridge
import com.metriql.warehouse.spi.querycontext.DependencyFetcher
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import org.rakam.server.http.HttpServer
import javax.inject.Inject

class DbtModelService @Inject constructor(
    private val renderer: JinjaRendererService,
    private val modelService: IDatasetService?,
    private val dependencyFetcher: DependencyFetcher
) {
    private val jinja = Jinjava()

    init {
        jinja.globalContext.registerFilter(object : Filter {
            override fun getName() = "toJson"
            override fun filter(param: Any?, p1: JinjavaInterpreter?, vararg p2: String?) = JsonHelper.encode(param)
        })
    }

    fun addDbtFiles(
        auth: ProjectAuth,
        committer: FileHandler,
        recipe: Recipe,
        dataSource: DataSource,
        recipeId: Int,
    ): List<HttpServer.JsonAPIError> {
        val directory = recipe.getDependenciesWithFallback().dbtDependency().aggregatesDirectory()
        committer.deletePath(directory)

        val (_, _, modelConfigMapper) = dataSource.dbtSettings()
        val models = recipe.models?.map { it.toModel(recipe.packageName ?: "", dataSource.warehouse.bridge, recipeId) } ?: listOf()
        val modelService = UpdatableDatasetService(modelService) { models }

        val context = QueryGeneratorContext(
            auth,
            dataSource,
            modelService,
            renderer,
            reportExecutor = null,
            userAttributeFetcher = null,
            dependencyFetcher = dependencyFetcher
        )

        val segmentationService = SegmentationService()
        val errors = mutableListOf<HttpServer.JsonAPIError>()

        recipe.models?.forEach { model ->
            model.getMaterializes().forEach { materialize ->
                val modelName = model.name!!
                val (target, rawRenderedSql) = try {
                    segmentationService.generateMaterializeQuery(auth.projectId, context, modelName, materialize.name, materialize.value)
                } catch (e: MetriqlException) {
                    errors.add(HttpServer.JsonAPIError.title("Unable to create materialize ${model.name}.${materialize.name}: $e"))
                    return@forEach
                }

                val renderedQuery = context.datasource.warehouse.bridge.generateQuery(context.viewModels, rawRenderedSql)

                val eventTimestamp = model.mappings?.get(EVENT_TIMESTAMP)

                val (materialized, renderedSql) = if (eventTimestamp != null) {
                    val eventTimestampDim = materialize.value.dimensions?.find { d -> d.name == eventTimestamp && d.relation == null }
                        ?.toDimension(modelName, FieldType.TIMESTAMP)!!
                    val eventDimensionAlias = context.getDimensionAlias(
                        eventTimestamp,
                        null,
                        eventTimestampDim.postOperation
                    )

                    val renderedEventTimestampDimension = dataSource.warehouse.bridge.renderDimension(
                        context, modelName, eventTimestamp, null, null,
                        WarehouseMetriqlBridge.MetricPositionType.FILTER
                    )

                    val query = """SELECT * FROM ($renderedQuery) AS $modelName
{% if is_incremental() %}
   WHERE ${renderedEventTimestampDimension.value} > (select max($eventDimensionAlias) from {{ this }})
{% endif %}
                    """.trimIndent()

                    "incremental" to query
                } else {
                    "table" to renderedQuery
                }

                val materializedModelName = generateModelName(modelName, materialize.reportType, materialize.name)
                val schema = recipe.getDependenciesWithFallback().dbtDependency().aggregateSchema()
                val config = modelConfigMapper.invoke(Triple(materializedModelName, target.copy(schema = schema), mapOf("materialized" to materialized)))
                val configs = jinja.render(CONFIG_TEMPLATE, mapOf("configs" to config, "tagName" to tagName))
                committer.addFile("$directory/$materializedModelName.sql", configs + "\n" + renderedSql)
            }
        }

        return errors
    }

    fun createProfiles(dataSource: DataSource, dependency: DbtDependency): String {
        val (dbType, credentials, _) = dataSource.dbtSettings()

        return YamlHelper.mapper.writeValueAsString(
            mapOf(
                dependency.profiles() to mapOf(
                    "target" to "prod",
                    "outputs" to
                        mapOf("prod" to mapOf("type" to dbType) + credentials)
                )
            )
        )
    }

    companion object {
        val CONFIG_TEMPLATE = this::class.java.getResource("/dbt/model.sql.jinja2").readText()
        const val tagName = "metriql_materialize"
        fun generateModelName(modelName: String, reportType: ReportType, name: String): String {
            return "${modelName}_${reportType.slug}_$name"
        }
    }
}
