package com.metriql.warehouse.metriql

import com.fasterxml.jackson.annotation.JsonTypeInfo.As.EXISTING_PROPERTY
import com.metriql.util.JsonHelper
import com.metriql.util.PolymorphicTypeStr
import com.metriql.util.StrValueEnum
import com.metriql.util.UppercaseEnum
import io.trino.connector.system.SystemTablesProvider
import io.trino.decoder.DispatchingRowDecoderFactory
import io.trino.decoder.RowDecoderFactory
import io.trino.decoder.avro.AvroBytesDeserializer
import io.trino.decoder.avro.AvroRowDecoderFactory
import io.trino.decoder.csv.CsvRowDecoderFactory
import io.trino.decoder.json.JsonRowDecoderFactory
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.connector.SystemTable
import java.util.Optional
import kotlin.reflect.KClass

data class CatalogFile(val version: String, val catalogs: Catalogs?) {
    class Catalogs : HashMap<String, Catalogs.Catalog>() {
        data class Catalog(
            val type: CatalogType,
            @PolymorphicTypeStr<CatalogType>(externalProperty = "type", inclusion = EXISTING_PROPERTY, valuesEnum = CatalogType::class)
            val value: CatalogValue
        )

        @UppercaseEnum
        enum class CatalogType(private val clazz: KClass<out CatalogValue>) : StrValueEnum {
            S3(CatalogValue.S3Catalog::class);

            override fun getValueClass() = clazz.java
        }

        sealed class CatalogValue : SystemTablesProvider {

            data class S3Catalog(
                val access_key: String?,
                val secret_key: String?,
                val bucket: String,
                val region: String?,
                val endpoint: String?,
                val path: String?,
                val fileType: FileType?,
                val schemas: Map<String, S3Catalog>?
            ) : CatalogValue() {
                enum class FileType(val factory: () -> RowDecoderFactory) {
                    JSON({ JsonRowDecoderFactory(JsonHelper.getMapper()) }),
                    CSV({ CsvRowDecoderFactory() }),
                    AVRO({
                        AvroRowDecoderFactory(
                            io.trino.decoder.avro.FixedSchemaAvroReaderSupplier.Factory(),
                            AvroBytesDeserializer.Factory()
                        )
                    });

                    companion object {
                        val factories = values().associate { it.name.lowercase() to it.factory.invoke() }
                    }
                }

                override fun listSystemTables(session: ConnectorSession): Set<SystemTable> {

                    val rowDecoderFactory = DispatchingRowDecoderFactory(FileType.factories)
                    val create = rowDecoderFactory.create("avro", mapOf("dataSchema" to null), setOf())
                    return null!!
//                    create.decodeRow(null).get().get(null).block
//
//                    if(schemas != null) {
//                        schemas.map { ExternalTable(session, it.value ?:) }
//                    } else {
//
//                    }
                }

                override fun getSystemTable(session: ConnectorSession, tableName: SchemaTableName?): Optional<SystemTable> {
                    return null!!
//                    return Optional.of(ExternalTable(session, this, tableName))
                }
            }
        }
    }
}
