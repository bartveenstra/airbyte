/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.parquet.AzureBlobStorageParquetFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureDataLakeStorageGen2FormatConfigs {

  private AzureDataLakeStorageGen2FormatConfigs() {

  }

  protected static final Logger LOGGER = LoggerFactory
      .getLogger(AzureDataLakeStorageGen2FormatConfigs.class);

  public static AzureDataLakeStorageGen2FormatConfig getAzureDataLakeStorageGen2FormatConfig(final JsonNode config) {
    final JsonNode formatConfig = config.get("format");
    LOGGER.info("Azure Blob Storage format config: {}", formatConfig.toString());
    final AzureDalaLakeStorageGen2Format formatType = AzureDalaLakeStorageGen2Format
        .valueOf(formatConfig.get("format_type").asText().toUpperCase());

    switch (formatType) {
      case PARQUET -> {
        return new AzureBlobStorageParquetFormatConfig();
      }
      default -> throw new RuntimeException("Unexpected output format: " + Jsons.serialize(config));

    }
  }

}
