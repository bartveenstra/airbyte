/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

import com.fasterxml.jackson.databind.JsonNode;

public interface AzureDataLakeStorageGen2FormatConfig {

  AzureDalaLakeStorageGen2Format getFormat();

  static String withDefault(final JsonNode config, final String property, final String defaultValue) {
    final JsonNode value = config.get(property);
    if (value == null || value.isNull()) {
      return defaultValue;
    }
    return value.asText();
  }

  static int withDefault(final JsonNode config, final String property, final int defaultValue) {
    final JsonNode value = config.get(property);
    if (value == null || value.isNull()) {
      return defaultValue;
    }
    return value.asInt();
  }

  static boolean withDefault(final JsonNode config, final String property, final boolean defaultValue) {
    final JsonNode value = config.get(property);
    if (value == null || value.isNull()) {
      return defaultValue;
    }
    return value.asBoolean();
  }

}
