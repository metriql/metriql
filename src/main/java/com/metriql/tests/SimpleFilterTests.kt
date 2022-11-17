package com.metriql.tests

import com.metriql.report.data.ReportFilter
import com.metriql.report.data.ReportFilter.FilterValue.MetricFilter
import com.metriql.report.data.ReportMetric.ReportDimension
import com.metriql.service.dataset.DatasetName
import com.metriql.warehouse.spi.filter.AnyOperatorType
import com.metriql.warehouse.spi.filter.BooleanOperatorType
import com.metriql.warehouse.spi.filter.DateOperatorType
import com.metriql.warehouse.spi.filter.NumberOperatorType
import com.metriql.warehouse.spi.filter.StringOperatorType
import com.metriql.warehouse.spi.filter.TimestampOperatorType
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

object SimpleFilterTests {

    // 1000 * 60 * 60 = 1 Hour
    val testInt = listOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val testString = listOf("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliett")
    val testBool = listOf(false, true, true, true, true, true, true, true, true, true)
    val testDouble = testInt.map { it * 1.0 }
    val testDate = testInt.map { LocalDate.of(2000, 1, it + 1) }
    val testTimestamp = testInt.map { LocalDateTime.ofEpochSecond((it * 60 * 60).toLong(), 0, ZoneOffset.UTC) }

    interface OperatorTests {
        fun filter(datasetName: DatasetName): List<ReportFilter>
        val result: Any?
    }

    enum class ComplexTest : OperatorTests {
        COMPLEX_1 {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.EQUALS.name, 1
                                ),
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.EQUALS.name, 2
                                ),
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.EQUALS.name, 3
                                )
                            )
                        )
                    ),
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.EQUALS.name, "charlie"
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("charlie")
        },
        COMPLEX_2 {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.EQUALS.name, 1
                                ),
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.EQUALS.name, 2
                                ),
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.LESS_THAN.name, 6
                                )
                            )
                        )
                    ),
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.EQUALS.name, "charlie"
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("charlie")
        }
    }

    enum class AnyOperatorTest : OperatorTests {
        IS_SET {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), AnyOperatorType.IS_SET.name, null
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf(testString[0])
        },

        IS_NOT_SET {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), AnyOperatorType.IS_NOT_SET.name, null
                                )
                            )
                        )
                    )
                )
            }

            override val result = null
        };
    }

    enum class StringOperatorTest : OperatorTests {
        EQUALS {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,
                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.EQUALS.name, "alpha"
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("alpha")
        },

        NOT_EQUALS {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,
                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.NOT_EQUALS.name, "alpha"
                                )
                            )
                        ),
                    ),
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,
                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.EQUALS.name, "bravo"
                                )
                            )
                        ),
                    )
                )
            }

            override val result: List<String> = listOf("bravo")
        },

        CONTAINS {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.CONTAINS.name, "liet"
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("juliett")
        },

        STARTS_WITH {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.STARTS_WITH.name, "charli"
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("charlie")
        },

        ENDS_WITH {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.ENDS_WITH.name, "trot"
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("foxtrot")
        },

        IN {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.IN.name, listOf("alpha")
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("alpha")
        },

        NOT_IN {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.NOT_IN.name, listOf("alpha")
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("alpha")
        },

        EQUALS_MULTI {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_string", datasetName, null, null), StringOperatorType.IN.name, listOf("alpha")
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<String> = listOf("alpha")
        },
    }

    enum class BooleanTest : OperatorTests {
        EQUALS {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_bool", datasetName, null, null), BooleanOperatorType.EQUALS.name, true
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(9.0)
        },
        NOT_EQUALS {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_bool", datasetName, null, null), BooleanOperatorType.NOT_EQUALS.name, false
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(9.0)
        }
    }

    enum class NumberTest : OperatorTests {
        EQUALS_INT {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_double", datasetName, null, null), NumberOperatorType.EQUALS.name, 1
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(1.0)
        },

        EQUALS_DOUBLE {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_double", datasetName, null, null), NumberOperatorType.EQUALS.name, 1.0
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(1.0)
        },

        GREATER_THAN_INT {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.GREATER_THAN.name, 0
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(1.0)
        },

        GREATER_THAN_DOUBLE {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_double", datasetName, null, null), NumberOperatorType.GREATER_THAN.name, 0
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(1.0)
        },

        LESS_THAN_INT {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.LESS_THAN.name, 1
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(0.0)
        },

        LESS_THAN_DOUBLE {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_double", datasetName, null, null), NumberOperatorType.LESS_THAN.name, 1.0
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(0.0)
        },

        GREATER_THAN_AND_LESS_THAN_INT {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.GREATER_THAN.name, 3
                                )
                            )
                        )
                    ),
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_int", datasetName, null, null), NumberOperatorType.LESS_THAN.name, 5
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(4.0)
        },

        GREATER_THAN_AND_LESS_THAN_DOUBLE {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_double", datasetName, null, null), NumberOperatorType.GREATER_THAN.name, 3.0
                                )
                            )
                        )
                    ),
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_double", datasetName, null, null), NumberOperatorType.LESS_THAN.name, 5.0
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(4.0)
        };
    }

    enum class TimestampOperatorTest : OperatorTests {
        GREATER_THAN {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_timestamp", datasetName, null, null),
                                    TimestampOperatorType.GREATER_THAN.name,
                                    testTimestamp.last().format(DateTimeFormatter.ISO_DATE_TIME)
                                )
                            )
                        )
                    )
                )
            }

            override val result = null
        },

        LESS_THAN {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_timestamp", datasetName, null, null),
                                    TimestampOperatorType.LESS_THAN.name,
                                    testTimestamp[1].toString()
                                )
                            )
                        )
                    )
                )
            }

            override val result: List<LocalDateTime> = listOf(testTimestamp.first())
        },

        GREATER_THAN_AND_LESS_THAN {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_timestamp", datasetName, null, null),
                                    TimestampOperatorType.GREATER_THAN.name,
                                    testTimestamp[3].toString()
                                )
                            )
                        )
                    ),
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_timestamp", datasetName, null, null),
                                    TimestampOperatorType.LESS_THAN.name,
                                    testTimestamp[5].toString()
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(testTimestamp[4])
        },
    }

    enum class DateOperatorTests : OperatorTests {
        GREATER_THAN {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,
                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_date", datasetName, null, null), DateOperatorType.GREATER_THAN.name, testDate.last().toString()
                                )
                            )
                        )
                    )
                )
            }

            override val result = null
        },

        LESS_THAN {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_date", datasetName, null, null), DateOperatorType.LESS_THAN.name, testDate[1].toString()
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(testDate.first())
        },

        GREATER_THAN_AND_LESS_THAN {
            override fun filter(datasetName: DatasetName): List<ReportFilter> {
                return listOf(
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(
                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_date", datasetName, null, null),
                                    DateOperatorType.GREATER_THAN.name,
                                    testDate[3].toString()
                                )
                            )
                        )
                    ),
                    ReportFilter(
                        ReportFilter.Type.METRIC,
                        MetricFilter(
                            MetricFilter.Connector.AND,

                            listOf(

                                MetricFilter.Filter(
                                    MetricFilter.MetricType.DIMENSION,
                                    ReportDimension("test_date", datasetName, null, null), DateOperatorType.LESS_THAN.name, testDate[5].toString()
                                )
                            )
                        )
                    )
                )
            }

            override val result = listOf(testDate[4])
        };
    }
}
