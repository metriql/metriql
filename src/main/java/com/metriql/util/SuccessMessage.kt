package com.metriql.util

import com.fasterxml.jackson.annotation.JsonInclude

data class SuccessMessage private constructor(@JsonInclude(JsonInclude.Include.NON_NULL) val message: String?) {

    val success = true

    companion object {
        private val SUCCESS = SuccessMessage(null)

        fun success(): SuccessMessage {
            return SUCCESS
        }

        fun success(message: String): SuccessMessage {
            return SuccessMessage(message)
        }
    }
}
