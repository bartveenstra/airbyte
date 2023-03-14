/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

public enum AzureDalaLakeStorageGen2Format {

  PARQUET("parquet"),

  AVRO("avro");

  private final String fileExtension;

  AzureDalaLakeStorageGen2Format(final String fileExtension) {
    this.fileExtension = fileExtension;
  }

  public String getFileExtension() {
    return fileExtension;
  }

}
