package com.metriql.util

import io.netty.handler.codec.http.HttpResponseStatus
import org.rakam.server.http.HttpRequestException
import org.rakam.server.http.HttpServer.JsonAPIError

class MetriqlException : HttpRequestException {

    private var isUserException: Boolean = false
    var predefinedException: MetriqlExceptions? = null

    constructor(message: String?, statusCode: HttpResponseStatus, userException: Boolean?) : super(
        message,
        statusCode
    ) {
        this.isUserException = userException ?: false
    }

    constructor(message: String?, statusCode: HttpResponseStatus) : super(message, statusCode)

    constructor(errors: List<JsonAPIError>, meta: Map<String, Any>, statusCode: HttpResponseStatus) : super(
        errors,
        meta,
        statusCode
    )

    constructor(statusCode: HttpResponseStatus) : super(statusCode.reasonPhrase(), statusCode)

    constructor(predefinedException: MetriqlExceptions) : super(
        predefinedException.message,
        predefinedException.httpCode
    ) {
        this.predefinedException = predefinedException
    }

    override fun toString(): String {
        val message = super.getErrors().map { return it.title }.joinToString(", ")
        if (message.isEmpty()) {
            return "An undefined error occurred"
        }
        return message
    }
}
