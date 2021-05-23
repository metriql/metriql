package com.metriql.util

import com.fasterxml.jackson.core.JsonProcessingException
import kong.unirest.ObjectMapper
import kong.unirest.Unirest
import java.io.IOException

object UnirestHelper {
    val unirest = Unirest.spawnInstance()!!

    init {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                super.run()
                unirest.shutDown()
                Unirest.shutDown()
            }
        })
        unirest.config()
            .connectTimeout(1_200_000)
            .socketTimeout(2_800_000)
            /** Setting the object mapper is required to use our own helpers and annotations */
            .objectMapper = object : ObjectMapper {
            private val jacksonObjectMapper = JsonHelper.getMapper().copy()

            override fun <T> readValue(value: String, valueType: Class<T>): T {
                try {
                    return jacksonObjectMapper.readValue(value, valueType)
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            }

            override fun writeValue(value: Any): String {
                try {
                    return jacksonObjectMapper.writeValueAsString(value)
                } catch (e: JsonProcessingException) {
                    throw RuntimeException(e)
                }
            }
        }
    }
}
