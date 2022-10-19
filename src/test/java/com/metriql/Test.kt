package com.metriql

import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator
import com.metriql.db.FieldType
import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportMetric
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationReportOptions
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jdbc.IsMetriqlQueryVisitor
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Model
import com.metriql.util.JsonHelper
import com.metriql.warehouse.postgresql.PostgresqlMetriqlBridge
import com.metriql.warehouse.spi.filter.NumberOperatorType
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import io.swagger.parser.OpenAPIParser
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.parser.core.models.ParseOptions
import io.trino.sql.MetriqlSqlFormatter
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import org.openapitools.codegen.ClientOptInput
import org.openapitools.codegen.CodegenConfig
import org.openapitools.codegen.DefaultGenerator
import org.openapitools.codegen.cmd.Generate
import org.openapitools.codegen.cmd.GlobalOptions
import org.openapitools.codegen.cmd.OpenApiGeneratorCommand
import org.openapitools.codegen.languages.OpenAPIGenerator
import org.openapitools.codegen.languages.TypeScriptAxiosClientCodegen
import org.openapitools.codegen.languages.TypeScriptClientCodegen
import org.testng.annotations.Test
import java.io.File
import java.util.concurrent.atomic.AtomicReference

class Test {
    val sqlParser = SqlParser()

    val models = listOf(
        Recipe.RecipeModel(
            "tbl1",
            false,
            null,
            "select 2 as a, 4 as b",
            null,
            measures = mapOf("sum_of_a" to Recipe.RecipeModel.Metric.RecipeMeasure(aggregation = Model.Measure.AggregationType.SUM, dimension = "a")),
            dimensions = mapOf("a" to Recipe.RecipeModel.Metric.RecipeDimension(column = "a", type = FieldType.STRING)),
            relations = mapOf("tbl2" to Recipe.RecipeModel.RecipeRelation(source = "a", target = "a", model = "tbl2"))
        ),
        Recipe.RecipeModel(
            "tbl2",
            false,
            null,
            "select 1 as a",
            null,
            dimensions = mapOf("a" to Recipe.RecipeModel.Metric.RecipeDimension(sql = "{{this}}.a * 2", type = FieldType.STRING)),
        )
    ).map { it.toModel("", PostgresqlMetriqlBridge) }

    val metadataSql = "SELECT table_name FROM information_schema.tables"

    private val metriqlSimpleSql = """
        select tbl1.a, sum(tbl1.a) from tbl1 
        where tbl1.a = 100 
        group by 1
        order by 2 desc
    """.trimIndent()

    private val metriqlSql = """
        select tbl1.a, tbl1.a * 2, sum(tbl1.a) from tbl1 
        where tbl1.a * 2 = 100 
        group by 1,2 
        HAVING (COUNT(1) > 0)
        order by 3 desc
    """.trimIndent()

    @Test
    fun visitorTest() {
        val stmt = sqlParser.createStatement(metriqlSql, ParsingOptions())
        val isMetadata = AtomicReference<Boolean?>()
        IsMetriqlQueryVisitor("metriql").process(stmt, isMetadata)
    }

    @Test
    fun printJsonSchema() {
        val schemaGen = JsonSchemaGenerator(JsonHelper.getMapper())
        val schema = schemaGen.generateSchema(Recipe.RecipeModel::class.java)
        println(JsonHelper.encode(schema))
    }

    @Test
    fun printSegmentationConversion() {

        val a = JsonHelper.read(
            """
           {
             "modelName": "da69fe46-d74a-4ee0-bf79-d0bc621f017d",
             "measures": [],
             "dimensions": [],
             "filters": {
                 "type": "groupFilter",
                 "connector": "and",
                 "value":  {
                     "type": "metricFilter",
                     "value": {
                       "connector": "or",
                       "filters": [
                         {
                           "metricType": "dimension",
                           "metricValue": {
                             "name": "RAMP_ID",
                             "relationName": null,
                             "modelName": "da69fe46-d74a-4ee0-bf79-d0bc621f017d"
                           },
                           "valueType": "string",
                           "operator": "in",
                           "value": []
                         },
                         {
                             "metricType": "dimension",
                             "metricValue": {
                                 "name": "CAR_MAKE",
                                 "relationName": null,
                                 "modelName": "da69fe46-d74a-4ee0-bf79-d0bc621f017d"
                               },
                           "valueType": "string",
                           "operator": "in",
                           "value": []
                         }
                       ]
                     }
                 }
               },
               {
                 "type": "metricFilter",
                 "value": {
                   "connector": "and",
                   "filters": [
                     {  
                       "metricType": "dimension",
                       "metricValue": {
                         "name": "GENDER",
                         "relationName": null,
                         "modelName": "da69fe46-d74a-4ee0-bf79-d0bc621f017d"
                       },
                       "valueType": "string",
                       "operator": "in",
                       "value": []
                     }
                   ]
                 }
               }
             ]
           }
        """.trimIndent(), SegmentationReportOptions::class.java
        )

        println(JsonHelper.encode(a, true))
        println(JsonHelper.encode(a.toRecipeQuery()))

        val clean = """{
               "dataset":"da69fe46-d74a-4ee0-bf79-d0bc621f017d",
               "measures":[],
               "dimensions":[],
               "filters":{  
                  {
                     "dimension":"RAMP_ID",
                     "operator":"in",
                     "value":[]
                  },
                  {
                     "dimension":"CAR_MAKE",
                     "operator":"in",
                     "value":[]
                  },
                  {
                     "dimension":"GENDER",
                     "operator":"in",
                     "value":[]
                  }
               ]
            }"""
                }

    @Test
    fun segmentationRewriter() {
        val stmt = sqlParser.createStatement(metriqlSql, ParsingOptions())
        val context = QueryGeneratorContext(
            ProjectAuth.singleProject(null),
            null!!,
            null!!,
            JinjaRendererService(),
            null,
            null,
            null
        )
        val output = MetriqlSqlFormatter.formatSql(stmt, null!!, context, null)
        println(output)
    }

    @Test
    fun segmentatisonRewriter() {

        val readText = File("/Users/bkabak/Code/rakam-subproject/metriql/static/schema/openapi.json").bufferedReader().readText()
        val readContents = OpenAPIParser().readContents(readText, listOf(), ParseOptions())
        val api = readContents.openAPI

        val typeScriptAxiosClientCodegen = TypeScriptAxiosClientCodegen()
        typeScriptAxiosClientCodegen.outputDir = "/Users/bkabak/Code/rakam-subproject/rakam-bi-backend/metriql/client"
        val generate = DefaultGenerator().opts(ClientOptInput().openAPI(api).config(typeScriptAxiosClientCodegen)).generate()
        println(generate)
    }
}
