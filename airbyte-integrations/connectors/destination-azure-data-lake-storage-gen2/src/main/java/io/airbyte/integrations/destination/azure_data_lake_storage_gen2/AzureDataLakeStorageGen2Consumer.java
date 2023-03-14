/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.FailureTrackingAirbyteMessageConsumer;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.writer.AzureBlobStorageWriter;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.writer.AzureBlobStorageWriterFactory;
import io.airbyte.protocol.models.v0.*;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AzureDataLakeStorageGen2Consumer extends FailureTrackingAirbyteMessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureDataLakeStorageGen2Consumer.class);
  private static final String YYYY_MM_DD_FORMAT_STRING = "yyyy_MM_dd";

  private final AzureDataLakeStorageGen2DestinationConfig azureDataLakeStorageGen2DestinationConfig;
  private final ConfiguredAirbyteCatalog configuredCatalog;
  private final AzureBlobStorageWriterFactory writerFactory;
  private final Consumer<AirbyteMessage> outputRecordCollector;
  private final Map<AirbyteStreamNameNamespacePair, AzureBlobStorageWriter> streamNameAndNamespaceToWriters;

  public AzureDataLakeStorageGen2Consumer(
                                  final AzureDataLakeStorageGen2DestinationConfig azureDataLakeStorageGen2DestinationConfig,
                                  final ConfiguredAirbyteCatalog configuredCatalog,
                                  final AzureBlobStorageWriterFactory writerFactory,
                                  final Consumer<AirbyteMessage> outputRecordCollector) {
    this.azureDataLakeStorageGen2DestinationConfig = azureDataLakeStorageGen2DestinationConfig;
    this.configuredCatalog = configuredCatalog;
    this.writerFactory = writerFactory;
    this.outputRecordCollector = outputRecordCollector;
    this.streamNameAndNamespaceToWriters = new HashMap<>(configuredCatalog.getStreams().size());
  }

  @Override
  protected void startTracked() throws Exception {
    // Init the client builder itself here
    final DataLakeServiceClientBuilder dataLakeClientBuilder =
        AzureDataLakeStorageGen2DestinationConfig.createDataLakeClientBuilder(azureDataLakeStorageGen2DestinationConfig);
    final Configuration hadoopConfig = AzureDataLakeStorageGen2DestinationConfig.getHadoopConfiguration(azureDataLakeStorageGen2DestinationConfig);

    for (final ConfiguredAirbyteStream configuredStream : configuredCatalog.getStreams()) {

      Timestamp timestamp = new Timestamp(System.currentTimeMillis());


      final String fileName = configuredStream.getStream().getName() + "/" +
          getOutputFilename(timestamp);



      final AzureBlobStorageWriter writer = writerFactory
          .create(azureDataLakeStorageGen2DestinationConfig, hadoopConfig, configuredStream, timestamp);

      final AirbyteStream stream = configuredStream.getStream();
      final AirbyteStreamNameNamespacePair streamNamePair = AirbyteStreamNameNamespacePair
          .fromAirbyteStream(stream);
      streamNameAndNamespaceToWriters.put(streamNamePair, writer);
    }
  }

  @Override
  protected void acceptTracked(final AirbyteMessage airbyteMessage) throws Exception {
    if (airbyteMessage.getType() == Type.STATE) {
      outputRecordCollector.accept(airbyteMessage);
      return;
    } else if (airbyteMessage.getType() != Type.RECORD) {
      return;
    }

    final AirbyteRecordMessage recordMessage = airbyteMessage.getRecord();
    final AirbyteStreamNameNamespacePair pair = AirbyteStreamNameNamespacePair
        .fromRecordMessage(recordMessage);

    if (!streamNameAndNamespaceToWriters.containsKey(pair)) {
      final String errMsg = String.format(
          "Message contained record from a stream that was not in the catalog. \ncatalog: %s , \nmessage: %s",
          Jsons.serialize(configuredCatalog), Jsons.serialize(recordMessage));
      LOGGER.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }

    try {
      streamNameAndNamespaceToWriters.get(pair).write(UUID.randomUUID(), recordMessage);

    } catch (final Exception e) {
      LOGGER.error(String.format("Failed to write message for stream %s, details: %s",
          streamNameAndNamespaceToWriters.get(pair), e.getMessage()));
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void close(final boolean hasFailed) throws Exception {
    for (final AzureBlobStorageWriter handler : streamNameAndNamespaceToWriters.values()) {
      handler.close(hasFailed);
    }
  }

  private static String getOutputFilename(final Timestamp timestamp) {
    final DateFormat formatter = new SimpleDateFormat(YYYY_MM_DD_FORMAT_STRING);
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return String.format(
        "%s_%d_0",
        formatter.format(timestamp),
        timestamp.getTime());
  }

}
