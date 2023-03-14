/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;


public class AzureDataLakeStorageGen2DestinationConfig {

  private final String authenticationMode;
  private final String sasToken;
  private final String storageAccessKey;
  private final String accountName;
  private final String containerName;
  private final String basePath;
  private final AzureDataLakeStorageGen2FormatConfig formatConfig;

  public AzureDataLakeStorageGen2DestinationConfig(
          String authenticationMode,
          String sasToken,
          String storageAccessKey,
          String accountName,
          String containerName,
          String basePath,
          AzureDataLakeStorageGen2FormatConfig formatConfig) {
    this.authenticationMode = authenticationMode;
    this.sasToken = sasToken;
    this.storageAccessKey = storageAccessKey;
    this.accountName = accountName;
    this.containerName = containerName;
    this.basePath = basePath;
    this.formatConfig = formatConfig;
  }

  public AzureDataLakeStorageGen2FormatConfig getFormatConfig() {
    return formatConfig;
  }

  public static DataLakeServiceClientBuilder createDataLakeClientBuilder(AzureDataLakeStorageGen2DestinationConfig destinationConfig) {

    String endpoint = "https://" + destinationConfig.accountName + ".dfs.core.windows.net/" + destinationConfig.getContainerName();

    DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
    if (destinationConfig.getAuthenticationMode().equals("SAS")) {
      builder.credential(new AzureSasCredential(destinationConfig.getSasToken()));
    } else if (destinationConfig.getAuthenticationMode().equals("SAK")) {
      builder.credential(new StorageSharedKeyCredential(destinationConfig.getAccountName(),  destinationConfig.getStorageAccessKey()));
    }
    builder.endpoint(endpoint);
    return builder;

  }

  public static Configuration getHadoopConfiguration(AzureDataLakeStorageGen2DestinationConfig destinationConfig) {
    final Configuration hadoopConfig = new Configuration();
    hadoopConfig.set("fs.azure.account.key.%s.blob.core.windows.net".formatted(destinationConfig.getAccountName()), destinationConfig.getContainerName());
    return hadoopConfig;
  }

  public static AzureDataLakeStorageGen2DestinationConfig getAzureBlobStorageConfig(final JsonNode config) {
    final JsonNode authenticationModeFromConfig = config.get("azure_data_lake_storage_gen2_authentication_mode");
    final String authenticationMode = authenticationModeFromConfig.get("authentication_mode").asText();
    final String sasToken = authenticationModeFromConfig.has("sas_token") ? authenticationModeFromConfig.get("sas_token").asText() : null;
    final String storageAccessKey = authenticationModeFromConfig.has("storage_account_key") ? authenticationModeFromConfig.get("storage_account_key").asText() : null;

    final String accountName = config.get("azure_data_lake_storage_gen2_storage_account_name").asText();
    final String containerName = config.get("azure_data_lake_storage_gen2_container_name").asText();
    final String basePath = config.get("azure_data_lake_storage_gen2_base_path").asText();

    return new AzureDataLakeStorageGen2DestinationConfig(
            authenticationMode,
            sasToken,
            storageAccessKey,
            accountName,
            containerName,
            basePath,
            AzureDataLakeStorageGen2FormatConfigs.getAzureDataLakeStorageGen2FormatConfig(config));

  }

  public String getAccountName() {
    return accountName;
  }

  public String getContainerName() {
    return containerName;
  }

  public String getBasePath() {
    return basePath;
  }

  public String getAuthenticationMode() {
    return authenticationMode;
  }

  public String getSasToken() {
    return sasToken;
  }

  public String getStorageAccessKey() {
    return storageAccessKey;
  }
}
