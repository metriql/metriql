package com.metriql.warehouse.metriql.storage

import io.airlift.units.DataSize
import java.io.File

class StorageConfig {
    var cacheDataSize = DataSize.of(1, DataSize.Unit.GIGABYTE)
    var cacheDataDirectory = File("./cache")
}
