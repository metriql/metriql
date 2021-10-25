package com.metriql.warehouse.metriql.storage

import java.io.InputStream

interface ObjectStore {
    fun list(path: String): Iterable<String>

    fun get(path: String): InputStream?
}
