package com.metriql.util

import com.hubspot.jinjava.interpret.FatalTemplateErrorsException
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import org.rakam.server.http.HttpServer

enum class MetriqlExceptions {

    // MARK: - System - ACL Exceptions

    ACCESS_DENIED {
        override val internalCode: Int = 900
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "Server is not configured or you don't have access for this feature"
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    ACCESS_DENIED_SHARE {
        override val internalCode: Int = 901
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "You don't have permission to share your reports and dashboards with everyone in your team."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    SYSTEM_CANT_SEND_EMAIL {
        override val internalCode: Int = 902
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "An error occurred while sending e-mail, please contact your administrator."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    SYSTEM_EXCEPTION_FROM_CAUSE {
        override val internalCode: Int = 903
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = null
        override val exception: MetriqlException
            get() = throw UnsupportedOperationException("This is a custom exception.")

        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException {
            return when (e) {
                is MetriqlException -> {
                    return e
                }
                is Throwable -> {
                    e.cause?.let {
                        return MetriqlException(it.message ?: "Unknown error caused by ${e.cause}", this.httpCode, isUserException)
                    } ?: run {
                        return MetriqlException(e.message ?: "Unknown error caused by $e", this.httpCode, isUserException)
                    }
                }
                else -> throw UnsupportedOperationException("Only Throwable are supported.")
            }
        }
    },

    SYSTEM_FILTER_TYPE_CANNOT_CAST {
        override val internalCode: Int = 904
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = null
        override val exception: MetriqlException
            get() = throw UnsupportedOperationException("This is a custom exception.")

        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException {
            return exceptionFromObject(e as Pair<Any?, Class<*>>, isUserException)
        }

        private fun exceptionFromObject(e: Pair<Any?, Class<*>>, isUserException: Boolean): MetriqlException {
            return MetriqlException(
                "Filter value '${e.first}' is not valid for the field, expected type is '${e.second}'",
                this.httpCode,
                isUserException
            )
        }
    },

    SYSTEM_NOT_ALLOWED_TO_CREATE_PROJECT {
        override val internalCode: Int = 905
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "You are not allowed to create projects."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    JINJA_TEMPLATE_EXCEPTION {
        override val internalCode: Int = 909
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = null
        override val exception: MetriqlException
            get() = throw UnsupportedOperationException("This is a custom exception.")

        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException {
            when (e) {
                is FatalTemplateErrorsException -> {
                    fun extractErrors(e: FatalTemplateErrorsException): List<HttpServer.JsonAPIError> {
                        return e.errors.map {
                            val error = HttpServer.JsonAPIError(
                                null,
                                listOf("https://docs.rakam.io/docs/sql-context#section-measures-dimensions-and-relations"),
                                null,
                                "$internalCode",
                                "Error while rendering template '${e.template}'",
                                it.message,
                                null
                            )
                            val causeErrors = if (it.exception is FatalTemplateErrorsException) {
                                extractErrors(it.exception as FatalTemplateErrorsException)
                            } else {
                                listOf()
                            }
                            val mergedErrors = causeErrors.toMutableList()
                            mergedErrors.add(error)
                            mergedErrors
                        }.flatten()
                    }
                    return MetriqlException(extractErrors(e), mapOf(), this.httpCode)
                }
                else -> throw UnsupportedOperationException("Only SqlExceptions are supported.")
            }
        }
    },

    // MARK: - Report Exceptions

    REPORT_NOT_FOUND {
        override val internalCode: Int = 2000
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "Report not found."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    // MARK: - User - Dashboard - Team Service

    USER_TEAM_ALREADY_IN_PROJECT {
        override val internalCode: Int = 3000
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "User is already in the team."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    USER_TEAM_OWNER_OF_PROJECT {
        override val internalCode: Int = 3001
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "User is owner of this project."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    USER_DASHBOARD_ACCESS_DENIED {
        override val internalCode: Int = 3003
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "Dashboard not found or you don't have access."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    // MARK: - QUERY related services

    QUERY_NO_RESULT {
        override val internalCode: Int = 4001
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "No result were returned by query."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    MODEL_NOT_EXISTS {
        override val internalCode: Int = 9005
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "Model does not exists"
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    },

    GITHUB_ACCESS_NOT_AVAILABLE {
        override val internalCode: Int = 9006
        override val httpCode: HttpResponseStatus = BAD_REQUEST
        override val message: String? = "Github organization configurations are not set."
        override val exception: MetriqlException get() = MetriqlException(this)
        override fun exceptionFromObject(e: Any, isUserException: Boolean): MetriqlException =
            throw UnsupportedOperationException("Custom exceptions are not supported by ${this.name}")
    };

    abstract val internalCode: Int
    abstract val httpCode: HttpResponseStatus
    abstract val message: String?
    abstract val exception: MetriqlException
    abstract fun exceptionFromObject(e: Any, isUserException: Boolean = false): MetriqlException
}
