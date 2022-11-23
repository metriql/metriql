package com.metriql

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator
import com.metriql.db.FieldType
import com.metriql.report.data.FilterValue
import com.metriql.report.data.recipe.Recipe
import com.metriql.service.dataset.Dataset
import com.metriql.service.jdbc.IsMetriqlQueryVisitor
import com.metriql.util.JsonHelper
import com.metriql.warehouse.postgresql.PostgresqlMetriqlBridge
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import org.intellij.lang.annotations.Language
import org.testng.annotations.Test
import java.util.concurrent.atomic.AtomicReference

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
    fun main() {
        val json = "{\"and\": [{\"dimension\": \"test\", \"operator\": \"equals\", \"value\": \"test\"}, {\"or\": []}]}"
        val zooPen = JsonHelper.read(json, FilterValue::class.java)
        println(zooPen)
    }

    @Test
    fun small() {
        @Language("JSON5")
        val json = """
            {
               "and":[
                  {
                     "dimension":"gender",
                     "operator":"equals",
                     "value":"female"
                  },
                  {
                     "or":[
                        {
                           "dimension":"age",
                           "operator":"is_set"
                        },
                        {
                           "dimension":"country",
                           "operator":"equals",
                           "value":"UK"
                        }
                     ]
                  }
               ]
            }
        """.trimIndent()
        val zooPen = JsonHelper.read(json, FilterValue::class.java)
        println(zooPen)
    }

    @Test
    fun deductiona() {
    }

    @Test
    fun deduction() {
        val json = "[{\"wingspan\": 1}, {\"name\": \"equals\"}]"
        val zooPen = JsonHelper.read(json, object : com.fasterxml.jackson.core.type.TypeReference<List<Animal>>() {}) // Currently throws InvalidTypeIdException
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION, defaultImpl = Animal::class)
    @JsonSubTypes(*[JsonSubTypes.Type(value = Animal.Bird::class), JsonSubTypes.Type(value = Animal.WildAnimal::class)])
    sealed class Animal {

        class Bird : Animal() {
            var wingspan = 0.0
        }

        class WildAnimal : Animal() {
            var name = "mahmut"
        }
    }
}
