package com.metriql

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator
import com.metriql.db.FieldType
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.jdbc.IsMetriqlQueryVisitor
import com.metriql.service.model.Dataset
import com.metriql.service.model.DatasetName
import com.metriql.service.model.DimensionName
import com.metriql.util.JsonHelper
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import com.metriql.warehouse.postgresql.PostgresqlMetriqlBridge
import io.swagger.parser.OpenAPIParser
import io.swagger.v3.parser.core.models.ParseOptions
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import org.intellij.lang.annotations.Language
import org.openapitools.codegen.ClientOptInput
import org.openapitools.codegen.DefaultGenerator
import org.openapitools.codegen.languages.TypeScriptAxiosClientCodegen
import org.testng.Assert.assertEquals
import org.testng.annotations.Test
import java.io.File
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.KClass

class Test {
    val sqlParser = SqlParser()

    val datasets = listOf(
        Recipe.RecipeModel(
            "tbl1",
            false,
            null,
            "select 2 as a, 4 as b",
            null,
            measures = mapOf("sum_of_a" to Recipe.RecipeModel.Metric.RecipeMeasure(aggregation = Dataset.Measure.AggregationType.SUM, dimension = "a")),
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
    fun segmentatisonRewriter() {

        val readText = File("/Users/bkabak/Code/rakam-subproject/metriql/static/schema/openapi.json").bufferedReader().readText()
        val readContents = OpenAPIParser().readContents(readText, listOf(), ParseOptions())
        val api = readContents.openAPI

        val typeScriptAxiosClientCodegen = TypeScriptAxiosClientCodegen()
        typeScriptAxiosClientCodegen.outputDir = "/Users/bkabak/Code/rakam-subproject/rakam-bi-backend/metriql/client"
        val generate = DefaultGenerator().opts(ClientOptInput().openAPI(api).config(typeScriptAxiosClientCodegen)).generate()
        println(generate)
    }

    @Test
    fun testName() {
        @Language("JSON5")
        val s = """
            {
              "@type": "dimension",
              "metric": {
                "name": "tets",
                "dataset": "test"
              }
            }
        """
        val read = JsonHelper.read(s.trimIndent(), Selam::class.java)
        println(read)
    }

    data class Selam(
//        @SealedClassInference
        val metric: ReportMetric,
    ) {
        @UppercaseEnum
        enum class MetricType(private val clazz: KClass<out ReportMetric>) : StrValueEnum {
            DIMENSION(ReportMetric.ReportDimension::class);

            override fun getValueClass() = clazz.java
        }

        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
        sealed class ReportMetric {
            @JsonTypeName("dimension")
            data class ReportDimension(
                val name: DimensionName,
                val dataset: DatasetName
            ) : ReportMetric()
        }
    }

    @Test
    fun main() {
        val json = "{\"animals\": [{\"name\": \"Sparky\"}, {\"name\": \"Polly\", \"wingspan\": 5.25}]}"
        val zooPen: ZooPen = JsonHelper.getMapper().readValue(json, ZooPen::class.java) // Currently throws InvalidTypeIdException
        assertEquals(Animal::class.java, zooPen.animals!![0].javaClass)
        assertEquals(Animal.Bird::class.java, zooPen.animals!![1].javaClass)
    }

    class ZooPen {
        var animals: List<Animal>? = null
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION, defaultImpl = Animal::class)
//    @JsonSubTypes(JsonSubTypes.Type(value = Animal.Bird::class))
    sealed class  Animal {
        var name: String? = null

        class Bird : Animal() {
            var wingspan = 0.0
        }
    }


}
