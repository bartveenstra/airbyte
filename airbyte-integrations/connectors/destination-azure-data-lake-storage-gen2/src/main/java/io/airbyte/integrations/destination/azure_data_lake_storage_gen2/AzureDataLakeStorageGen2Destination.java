/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.writer.AzureBlobStorageWriterFactory;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.writer.ProductionWriterFactory;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class AzureDataLakeStorageGen2Destination extends BaseConnector implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureDataLakeStorageGen2Destination.class);

  public static void main(final String[] args) throws Exception {
    new IntegrationRunner(new AzureDataLakeStorageGen2Destination()).run(args);
  }

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    try {
      final AzureDataLakeStorageGen2ConnectionChecker client = new AzureDataLakeStorageGen2ConnectionChecker(
          AzureDataLakeStorageGen2DestinationConfig.getAzureBlobStorageConfig(config));
      client.attemptWriteAndDelete();
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (final Exception e) {
      LOGGER.error("Exception attempting to access the azure blob storage bucket: ", e);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage(
              "Could not connect to the azure blob storage with the provided configuration. \n" + e
                  .getMessage());
    }
  }

  @Override
  public AirbyteMessageConsumer getConsumer(final JsonNode config,
                                            final ConfiguredAirbyteCatalog configuredCatalog,
                                            final Consumer<AirbyteMessage> outputRecordCollector) {
    final AzureBlobStorageWriterFactory formatterFactory = new ProductionWriterFactory();
    return new AzureDataLakeStorageGen2Consumer(
        AzureDataLakeStorageGen2DestinationConfig.getAzureBlobStorageConfig(config), configuredCatalog,
        formatterFactory, outputRecordCollector);
  }

}
