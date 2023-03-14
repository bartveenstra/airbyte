/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus.Status;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AzureDataLakeStorageGen2AcceptanceTest {

  protected final String secretFilePath = "secrets/config.json";
  private JsonNode config = null;

  @BeforeEach
  public void beforeAll() {
    final JsonNode configFomSecrets = Jsons.deserialize(IOs.readFile(Path.of(secretFilePath)));
    config = Jsons.jsonNode(ImmutableMap.builder()
        .put("azure_data_lake_storage_gen2_authentication_mode", configFomSecrets.get("azure_data_lake_storage_gen2_authentication_mode"))
        .put("azure_data_lake_storage_gen2_storage_account_name", configFomSecrets.get("azure_data_lake_storage_gen2_storage_account_name"))
        .put("azure_data_lake_storage_gen2_container_name", configFomSecrets.get("azure_data_lake_storage_gen2_container_name"))
        .put("azure_data_lake_storage_gen2_base_path", configFomSecrets.get("azure_data_lake_storage_gen2_base_path"))
        .put("format", getParquetFormatConfig())
        .build());
  }

  @Test
  public void testCheck() {
    final AzureDataLakeStorageGen2Destination azureBlobStorageDestination = new AzureDataLakeStorageGen2Destination();
    final AirbyteConnectionStatus checkResult = azureBlobStorageDestination.check(config);

    assertEquals(Status.SUCCEEDED, checkResult.getStatus());
  }

  @Test
  public void testCheckInvalidAccountName() {
    final JsonNode invalidConfig = Jsons.jsonNode(ImmutableMap.builder()
        .put("azure_data_lake_storage_gen2_storage_account_name", "someInvalidName")
        .put("azure_data_lake_storage_gen2_container_name", config.get("azure_data_lake_storage_gen2_container_name"))
        .put("format", getParquetFormatConfig())
        .build());
    final AzureDataLakeStorageGen2Destination azureBlobStorageDestination = new AzureDataLakeStorageGen2Destination();
    final AirbyteConnectionStatus checkResult = azureBlobStorageDestination.check(invalidConfig);

    assertEquals(Status.FAILED, checkResult.getStatus());
  }

  private JsonNode getParquetFormatConfig() {
    return Jsons.deserialize("{\n"
        + "  \"format_type\": \"PARQUET\"\n"
        + "}");
  }

}
