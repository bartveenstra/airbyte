/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2.writer;

import com.azure.storage.blob.specialized.AppendBlobClient;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.AzureDataLakeStorageGen2DestinationConfig;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.AzureDalaLakeStorageGen2Format;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.parquet.AzureBlobStorageParquetWriter;
import io.airbyte.integrations.destination.s3.avro.AvroConstants;
import io.airbyte.integrations.destination.s3.avro.JsonToAvroSchemaConverter;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class ProductionWriterFactory implements AzureBlobStorageWriterFactory {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ProductionWriterFactory.class);

  @Override
  public AzureBlobStorageWriter create(final AzureDataLakeStorageGen2DestinationConfig config,
                                       final Configuration hadoopConfig,
                                       final ConfiguredAirbyteStream configuredStream,
                                       final Timestamp uploadTimestamp)
          throws Exception {
    final AzureDalaLakeStorageGen2Format format = config.getFormatConfig().getFormat();

    if (format == AzureDalaLakeStorageGen2Format.PARQUET || format == AzureDalaLakeStorageGen2Format.AVRO) {

      final AirbyteStream stream = configuredStream.getStream();
      LOGGER.info("Json schema for stream {}: {}", stream.getName(), stream.getJsonSchema());

      final JsonToAvroSchemaConverter schemaConverter = new JsonToAvroSchemaConverter();
      final Schema avroSchema = schemaConverter.getAvroSchema(stream.getJsonSchema(), stream.getName(), stream.getNamespace());

      LOGGER.info("Avro schema for stream {}: {}", stream.getName(), avroSchema.toString(false));


      LOGGER.debug("Picked up Parquet format writer");
      return new AzureBlobStorageParquetWriter(config, hadoopConfig, configuredStream, uploadTimestamp, avroSchema, AvroConstants.JSON_CONVERTER);
    }

    throw new RuntimeException("Unexpected AzureBlobStorage destination format: " + format);
  }

}
