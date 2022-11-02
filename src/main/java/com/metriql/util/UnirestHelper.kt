package com.metriql.util

import com.fasterxml.jackson.core.JsonProcessingException
import kong.unirest.Config
import kong.unirest.GenericType
import kong.unirest.HttpRequest
import kong.unirest.HttpRequestSummary
import kong.unirest.HttpResponse
import kong.unirest.Interceptor
import kong.unirest.ObjectMapper
import kong.unirest.Unirest
import java.io.IOException
import java.util.logging.Logger

object UnirestHelper {
    val unirest = Unirest.spawnInstance()!!
    private val logger = Logger.getLogger(this::class.java.name)

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
            .setObjectMapper( object : ObjectMapper {
            private val mapper = JsonHelper.getMapper().copy()

            override fun <T> readValue(value: String, valueType: Class<T>): T {
                try {
                    return mapper.readValue(value, valueType)
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            }

            override fun <T : Any?> readValue(value: String, genericType: GenericType<T>): T {
                try {
                    return mapper.readValue(value, mapper.constructType(genericType.type))
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            }

            override fun writeValue(value: Any): String {
                try {
                    return mapper.writeValueAsString(value)
                } catch (e: JsonProcessingException) {
                    throw RuntimeException(e)
                }
            }
        }).interceptor(object : Interceptor {
                override fun onRequest(request: HttpRequest<*>, config: Config?) {
                    logger.info("Sending request to ${request.url}")
                }

                override fun onResponse(response: HttpResponse<*>, request: HttpRequestSummary, config: Config?) {
                    //
                }
            })
    }
}
