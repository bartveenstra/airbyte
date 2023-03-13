/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_blob_storage.writer;

import com.azure.storage.blob.specialized.AppendBlobClient;
import io.airbyte.integrations.destination.azure_blob_storage.AzureBlobStorageDestinationConfig;
import io.airbyte.integrations.destination.azure_blob_storage.AzureBlobStorageFormat;
import io.airbyte.integrations.destination.azure_blob_storage.csv.AzureBlobStorageCsvWriter;
import io.airbyte.integrations.destination.azure_blob_storage.jsonl.AzureBlobStorageJsonlWriter;
import io.airbyte.integrations.destination.azure_blob_storage.parquet.AzureBlobStorageParquetWriter;
import io.airbyte.integrations.destination.s3.avro.AvroConstants;
import io.airbyte.integrations.destination.s3.avro.JsonToAvroSchemaConverter;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class ProductionWriterFactory implements AzureBlobStorageWriterFactory {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ProductionWriterFactory.class);

  @Override
  public AzureBlobStorageWriter create(final AzureBlobStorageDestinationConfig config,
                                       final AppendBlobClient appendBlobClient,
                                       final ConfiguredAirbyteStream configuredStream,
                                       final Timestamp uploadTimestamp)
      throws Exception {
    final AzureBlobStorageFormat format = config.getFormatConfig().getFormat();

    if (format == AzureBlobStorageFormat.CSV) {
      LOGGER.debug("Picked up CSV format writer");
      return new AzureBlobStorageCsvWriter(config, appendBlobClient, configuredStream);
    }

    if (format == AzureBlobStorageFormat.JSONL) {
      LOGGER.debug("Picked up JSONL format writer");
      return new AzureBlobStorageJsonlWriter(config, appendBlobClient, configuredStream);
    }

    if (format == AzureBlobStorageFormat.PARQUET || format == AzureBlobStorageFormat.AVRO) {

      final AirbyteStream stream = configuredStream.getStream();
      LOGGER.info("Json schema for stream {}: {}", stream.getName(), stream.getJsonSchema());

      final JsonToAvroSchemaConverter schemaConverter = new JsonToAvroSchemaConverter();
      final Schema avroSchema = schemaConverter.getAvroSchema(stream.getJsonSchema(), stream.getName(), stream.getNamespace());

      LOGGER.info("Avro schema for stream {}: {}", stream.getName(), avroSchema.toString(false));


      LOGGER.debug("Picked up Parquet format writer");
      return new AzureBlobStorageParquetWriter(config, configuredStream,  uploadTimestamp, avroSchema, AvroConstants.JSON_CONVERTER);
    }

    throw new RuntimeException("Unexpected AzureBlobStorage destination format: " + format);
  }

}
