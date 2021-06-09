package com.metriql

import com.metriql.db.FieldType
import com.metriql.report.Recipe
import com.metriql.service.auth.ProjectAuth
import com.metriql.service.jdbc.IsMetadataQueryVisitor
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.service.model.Model
import com.metriql.warehouse.postgresql.PostgresqlMetriqlBridge
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import io.trino.sql.MetriqlSqlFormatter
import io.trino.sql.parser.ParsingOptions
import io.trino.sql.parser.SqlParser
import org.testng.annotations.Test
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
            dimensions = mapOf("a" to Recipe.RecipeModel.Metric.RecipeDimension(sql = "{{this}}.a * 2", type = FieldType.STRING))
        )
    ).map { it.toModel("", PostgresqlMetriqlBridge, -1) }

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
        IsMetadataQueryVisitor().process(stmt, isMetadata)
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
        )
        val output = MetriqlSqlFormatter.formatSql(stmt, null!!, context, models)
        println(output)
    }
}
