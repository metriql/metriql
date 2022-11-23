package com.metriql.tests

import com.metriql.util.JsonHelper
import org.testng.Assert

object Helper {
    fun assertJsonEquals(actual: Any?, expected: Any?) {
        Assert.assertEquals(JsonHelper.encode(actual), JsonHelper.encode(expected))
    }

    fun assetEqualsCaseInsensitive(expected: String, actual: String) {
        Assert.assertTrue(expected.equals(actual, ignoreCase = true), "Expected $expected, actual $actual.")
    }
}
