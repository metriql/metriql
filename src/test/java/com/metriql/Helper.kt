package com.metriql

import com.metriql.util.JsonHelper
import org.testng.Assert

object Helper {
    fun assertJsonEquals(actual: Any?, expected: Any?) {
        Assert.assertEquals(JsonHelper.encode(actual), JsonHelper.encode(expected))
    }
}
