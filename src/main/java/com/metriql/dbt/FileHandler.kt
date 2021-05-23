package com.metriql.dbt

import java.io.File
import java.time.Instant

interface FileHandler {
    fun addFile(path: String, content: String)
    fun deletePath(path: String)
    fun deleteFile(path: String)
    fun commit(message: String): String?
    fun getContent(): RepositoryContent
    fun cancel()

    data class RepositoryContent(val commitId: String?, val commitedAt: Instant?, val files: Map<String, String>, val path: File?, val hasAccess: Boolean, val hasJobs: Boolean)
}
