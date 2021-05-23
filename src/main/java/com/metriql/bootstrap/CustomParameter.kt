package com.metriql.bootstrap

import org.rakam.server.http.HttpServerBuilder

class CustomParameter(val parameterName: String, val factory: HttpServerBuilder.IRequestParameterFactory)
