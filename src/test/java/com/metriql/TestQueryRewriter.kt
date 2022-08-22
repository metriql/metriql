package com.metriql

import com.metriql.tests.Helper.assertJsonEquals
import com.metriql.report.data.recipe.Recipe
import com.metriql.report.segmentation.SegmentationQueryReWriter
import com.metriql.report.segmentation.SegmentationRecipeQuery
import com.metriql.report.segmentation.SegmentationRecipeQuery.SegmentationMaterialize
import com.metriql.service.auth.ProjectAuth.Companion.systemUser
import com.metriql.service.jinja.JinjaRendererService
import com.metriql.tests.TestDatasetService
import com.metriql.util.JsonHelper
import com.metriql.warehouse.postgresql.PostgresqlDataSource
import com.metriql.warehouse.postgresql.PostgresqlWarehouse.PostgresqlConfig
import com.metriql.warehouse.spi.querycontext.QueryGeneratorContext
import org.intellij.lang.annotations.Language
import org.testng.Assert.assertEquals
import org.testng.Assert.assertNull
import org.testng.annotations.Test

class TestQueryRewriter {
    @Test
    fun testBasicNoDimension() {
        @Language("JSON5")
        val query = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"]
          }""",
            SegmentationRecipeQuery::class.java
        ).toReportOptions(rewriter.context)

        @Language("JSON5")
        val aggregate = JsonHelper.read(
            """{
            "measures": ["measure1"]
          }""",
            SegmentationMaterialize::class.java
        )

        val (materializeQuery, model) = rewriter.findOptimumPlan(query, listOf(Triple(query.modelName, "daily_views", aggregate)))!!

        assertEquals(model.name, "daily_views")

        @Language("JSON5")
        val result = """{
              "model" : "daily_views",
              "measures" : [ "measure1" ]
            }"""

        assertJsonEquals(materializeQuery.toRecipeQuery(), result)
    }

    @Test
    fun testDimension() {
        @Language("JSON5")
        val query = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"],
            "dimensions": ["dimension1"]
          }""",
            SegmentationRecipeQuery::class.java
        ).toReportOptions(rewriter.context)

        @Language("JSON")
        val aggregate = JsonHelper.read(
            """{
            "measures": ["measure1"],
            "dimensions": ["dimension1"]
          }""",
            SegmentationMaterialize::class.java
        )

        val (materializeQuery, model) = rewriter.findOptimumPlan(query, listOf(Triple(query.modelName, "daily_views", aggregate)))!!

        assertEquals(model.name, "daily_views")
        assertJsonEquals(
            materializeQuery.toRecipeQuery(),
            """{
              "model" : "daily_views",
              "measures" : [ "measure1" ],
              "dimensions": [ "dimension1" ]
            }"""
        )
    }

    @Test
    fun testFilterFullMatch() {
        @Language("JSON5")
        val query = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"],
            "dimensions": ["dimension1"],
            "filters": [{"dimension": "dimension1", "operator": "equals", "value": "test"}]
          }""",
            SegmentationRecipeQuery::class.java
        ).toReportOptions(rewriter.context)

        @Language("JSON5")
        val aggregate = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"],
            "dimensions": ["dimension1"],
            "filters": [{"dimension": "dimension1", "operator": "equals", "value": "test"}]
          }""",
            SegmentationMaterialize::class.java
        )

        val (materializeQuery, model) = rewriter.findOptimumPlan(query, listOf(Triple(query.modelName, "daily_views", aggregate)))!!

        assertEquals(model.name, "daily_views")
        assertJsonEquals(
            materializeQuery.toRecipeQuery(),
            """{
              "model" : "daily_views",
              "measures" : [ "measure1" ],
              "dimensions": [ "dimension1" ]
            }"""
        )
    }

    @Test
    fun testFilterInclusive() {
        @Language("JSON5")
        val query = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"],
            "dimensions": ["dimension1"],
            "filters": [
                {"dimension": "dimension1", "operator": "contains", "value": "ahmet"}, 
                {"dimension": "dimension1", "operator": "contains", "value": "mehmet"}
            ]
          }""",
            SegmentationRecipeQuery::class.java
        ).toReportOptions(rewriter.context)

        @Language("JSON5")
        val aggregate = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"],
            "dimensions": ["dimension1"],
            "filters": [{"dimension": "dimension1", "operator": "contains", "value": "ahmet"}]
          }""",
            SegmentationMaterialize::class.java
        )

        val (materializeQuery, model) = rewriter.findOptimumPlan(query, listOf(Triple(query.modelName, "daily_views", aggregate)))!!

        assertEquals(model.name, "daily_views")
        assertJsonEquals(
            materializeQuery.toRecipeQuery(),
            """[{
              "model" : "daily_views",
              "measures" : [ "measure1" ],
              "dimensions" : [ "dimension1" ],
              "filters" : [ {
                "dimension" : "dimension1",
                "operator" : "contains",
                "value" : "mehmet"
              } ]
            }]"""
        )
    }

    @Test
    fun testMeasureNotMatching() {
        @Language("JSON5")
        val query = JsonHelper.read(
            """{
            "measures": ["measure1", "measure2"],
            "dimensions": ["dimension1"]
          }""",
            SegmentationRecipeQuery::class.java
        ).toReportOptions(rewriter.context)

        @Language("JSON5")
        val aggregate = JsonHelper.read(
            """{
            "measures": ["measure1"],
            "dimensions": ["dimension1"]
          }""",
            SegmentationMaterialize::class.java
        )

        val result = rewriter.findOptimumPlan(query, listOf(Triple(query.modelName, "daily_views", aggregate)))
        assertNull(result)
    }

    @Test
    fun testDimensionNotMatching() {
        @Language("JSON5")
        val query = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"],
            "dimensions": ["dimension1", "dimension2"]
          }""",
            SegmentationRecipeQuery::class.java
        ).toReportOptions(rewriter.context)

        @Language("JSON5")
        val aggregate = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"],
            "dimensions": ["dimension1"]
          }""",
            SegmentationMaterialize::class.java
        )

        val result = rewriter.findOptimumPlan(query, listOf(Triple(query.modelName, "daily_views", aggregate)))
        assertNull(result)
    }

    @Test(enabled = false)
    fun testMeasureFromRelation() {
        @Language("JSON5")
        val query = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1", "test2.measure1"],
            "dimensions": ["dimension1"]
          }""",
            SegmentationRecipeQuery::class.java
        ).toReportOptions(rewriter.context)

        @Language("JSON5")
        val aggregate = JsonHelper.read(
            """{
            "measures": ["measure1", "test2.measure1"],
            "dimensions": ["dimension1"]
          }""",
            SegmentationMaterialize::class.java
        )

        val (materializeQuery, model) = rewriter.findOptimumPlan(query, listOf(Triple(query.modelName, "daily_views", aggregate)))!!

        assertEquals(model.name, "daily_views")
        assertJsonEquals(
            materializeQuery.toRecipeQuery(),
            """{
              "model" : "daily_views",
              "measures" : ["measure1", "test2.measure1"],
              "dimensions": [ "dimension1" ]
            }"""
        )
    }

    @Test(enabled = false)
    fun testDimensionFromRelation() {
        @Language("JSON5")
        val query = JsonHelper.read(
            """{
            "model":  "test1", 
            "measures": ["measure1"],
            "dimensions": ["dimension1", "test2.dimension1"]
          }""",
            SegmentationRecipeQuery::class.java
        ).toReportOptions(rewriter.context)

        @Language("JSON5")
        val aggregate = JsonHelper.read(
            """{
            "measures": ["measure1", "test2.measure1"],
            "dimensions": ["dimension1", "test2.dimension1"]
          }""",
            SegmentationMaterialize::class.java
        )

        val (materializeQuery, model) = rewriter.findOptimumPlan(query, listOf(Triple(query.modelName, "daily_views", aggregate)))!!

        assertEquals(model.name, "daily_views")
        assertJsonEquals(
            materializeQuery.toRecipeQuery(),
            """{
              "model" : "daily_views",
              "measures" : ["measure1"],
              "dimensions": ["dimension1", "test2.dimension1"]
            }"""
        )
    }

    private val rewriter: SegmentationQueryReWriter
        get() {
            val auth = systemUser("1", 1)
            val datasource = PostgresqlDataSource(PostgresqlConfig("127.0.0.1", 5432, "rakamtest", "public", "buremba", ""))
            val renderer = JinjaRendererService()
            val datasetService = TestDatasetService(
                listOf(
                    JsonHelper.read(
                        """{
                          "name": "test1",
                          "target": {
                            "table": "_table"
                          },
                          "relations": {
                            "test2": {
                              "sourceColumn": "testdimension",
                              "targetColumn": "testdimension"
                            }
                          },
                          "dimensions": {
                            "dimension1": {
                              "column": "testdimension",
                              "type": "string"
                            },
                            "dimension2": {
                              "column": "test2dimension",
                              "type": "string"
                            }
                          },
                          "measures": {
                            "measure1": {
                              "aggregation": "count"
                            },
                            "measure2": {
                              "aggregation": "count"
                            }
                          }
                        }""",
                        Recipe.RecipeModel::class.java
                    ).toModel("", datasource.warehouse.bridge, -1),
                    JsonHelper.read(
                        """{
                              "name": "test2",
                              "target": {
                                "table": "_table"
                              },
                              "dimensions": {
                                "dimension1": {
                                  "column": "testdimension",
                                  "type": "string"
                                },
                                "dimension2": {
                                  "column": "test2dimension",
                                  "type": "string"
                                }
                              },
                              "measures": {
                                "measure1": {
                                  "aggregation": "count"
                                },
                                "measure2": {
                                  "aggregation": "count"
                                }
                              }
                            }""",
                        Recipe.RecipeModel::class.java
                    ).toModel("", datasource.warehouse.bridge, -1)
                )
            )
            return SegmentationQueryReWriter(
                QueryGeneratorContext(auth, datasource, datasetService, renderer, reportExecutor = null, userAttributeFetcher = null, dependencyFetcher = null),
            )
        }
}
