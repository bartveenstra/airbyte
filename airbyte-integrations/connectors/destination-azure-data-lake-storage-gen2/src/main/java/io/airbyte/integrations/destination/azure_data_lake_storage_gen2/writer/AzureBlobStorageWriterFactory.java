/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2.writer;

import com.azure.storage.blob.specialized.AppendBlobClient;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.AzureDataLakeStorageGen2DestinationConfig;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import org.apache.hadoop.conf.Configuration;

import java.sql.Timestamp;

/**
 * Create different {@link AzureBlobStorageWriter} based on
 * {@link AzureDataLakeStorageGen2DestinationConfig}.
 */
public interface AzureBlobStorageWriterFactory {

  AzureBlobStorageWriter create(AzureDataLakeStorageGen2DestinationConfig config,
                                Configuration hadoopConfig,
                                ConfiguredAirbyteStream configuredStream,
                                Timestamp uploadTimestamp)
      throws Exception;

}
