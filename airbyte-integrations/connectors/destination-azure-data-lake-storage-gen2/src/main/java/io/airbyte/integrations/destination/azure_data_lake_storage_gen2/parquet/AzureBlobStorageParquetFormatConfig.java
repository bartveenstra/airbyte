/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2.parquet;

import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.AzureDalaLakeStorageGen2Format;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.AzureDataLakeStorageGen2FormatConfig;

public class AzureBlobStorageParquetFormatConfig implements AzureDataLakeStorageGen2FormatConfig {

  @Override
  public AzureDalaLakeStorageGen2Format getFormat() {
    return AzureDalaLakeStorageGen2Format.PARQUET;
  }

}
