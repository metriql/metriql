package com.metriql.tests

import com.metriql.db.FieldType
import com.metriql.service.dataset.Dataset
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

object SampleDataset {
    // 1000 * 60 * 60 = 1 Hour
    val testInt = listOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val testString = listOf("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliett")
    val testBool = listOf(false, true, true, true, true, true, true, true, true, true)
    val testDouble = testInt.map { it * 1.0 }
    val testDate = testInt.map { LocalDate.of(2000, 1, it + 1) }
    val testTimestamp = testInt.map { LocalDateTime.ofEpochSecond((it * 60 * 60).toLong(), 0, ZoneOffset.UTC) }

    val datasetService = TestDatasetService(
        listOf(
            Dataset(
                "filter_tests", false,
                Dataset.Target.initWithTable(null, "rakam_test", "filter_tests"),
                null, null, null, Dataset.MappingDimensions.build(Dataset.MappingDimensions.CommonMappings.TIME_SERIES to "_time"), listOf(),
                listOf(
                    Dataset.Dimension("test_int", Dataset.Dimension.Type.COLUMN, Dataset.Dimension.DimensionValue.Column("test_int"), fieldType = FieldType.INTEGER),
                    Dataset.Dimension("test_string", Dataset.Dimension.Type.COLUMN, Dataset.Dimension.DimensionValue.Column("test_string"), fieldType = FieldType.STRING),
                    Dataset.Dimension("test_double", Dataset.Dimension.Type.COLUMN, Dataset.Dimension.DimensionValue.Column("test_double"), fieldType = FieldType.DOUBLE),
                    Dataset.Dimension("test_date", Dataset.Dimension.Type.COLUMN, Dataset.Dimension.DimensionValue.Column("test_date"), fieldType = FieldType.DATE),
                    Dataset.Dimension("test_bool", Dataset.Dimension.Type.COLUMN, Dataset.Dimension.DimensionValue.Column("test_bool"), fieldType = FieldType.BOOLEAN),
                    Dataset.Dimension("test_timestamp", Dataset.Dimension.Type.COLUMN, Dataset.Dimension.DimensionValue.Column("test_timestamp"), fieldType = FieldType.TIMESTAMP),
                ),
                listOf(),
            )
        )
    )
}
