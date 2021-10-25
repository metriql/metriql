package com.metriql.warehouse.metriql.storage

import com.metriql.warehouse.metriql.CatalogFile.Catalogs.CatalogValue.S3Catalog
import net.snowflake.client.jdbc.internal.amazonaws.auth.AWSStaticCredentialsProvider
import net.snowflake.client.jdbc.internal.amazonaws.auth.BasicAWSCredentials
import net.snowflake.client.jdbc.internal.amazonaws.auth.InstanceProfileCredentialsProvider
import net.snowflake.client.jdbc.internal.amazonaws.client.builder.AwsClientBuilder
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ListObjectsRequest
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.transfer.TransferManager
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.transfer.TransferManagerBuilder
import java.io.InputStream

class S3ObjectStore(private val s3Catalog: S3Catalog, private val storageConfig: StorageConfig) : ObjectStore {
    val client: AmazonS3
    val transferManager: TransferManager

    init {
        val builder = AmazonS3Client.builder()
        s3Catalog.region?.let { builder.region = it }
        s3Catalog.endpoint?.let { builder.setEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(it, builder.region)) }
        val credentials = if (s3Catalog.access_key != null) {
            AWSStaticCredentialsProvider(BasicAWSCredentials(s3Catalog.access_key, s3Catalog.secret_key))
        } else {
            InstanceProfileCredentialsProvider(false)
        }
        builder.withCredentials(credentials)
        client = builder.build()
        transferManager = TransferManagerBuilder.standard().withS3Client(client).build()
    }

    override fun list(path: String): Iterable<String> {
        val request = ListObjectsRequest(s3Catalog.bucket, (s3Catalog.path ?: "") + path, null, null, 1000)
        val objects = client.listObjects(request)
        return null!!
//        return objects.objectSummaries.map { ExternalTable() }
    }

    override fun get(path: String): InputStream {
        return client.getObject(s3Catalog.bucket, (s3Catalog.path ?: "") + path).objectContent
    }
}
