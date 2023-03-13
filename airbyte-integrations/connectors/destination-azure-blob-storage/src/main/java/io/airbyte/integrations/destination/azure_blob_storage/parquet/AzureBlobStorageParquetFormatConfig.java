/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_blob_storage.parquet;

import io.airbyte.integrations.destination.azure_blob_storage.AzureBlobStorageFormat;
import io.airbyte.integrations.destination.azure_blob_storage.AzureBlobStorageFormatConfig;

public class AzureBlobStorageParquetFormatConfig implements AzureBlobStorageFormatConfig {

  @Override
  public AzureBlobStorageFormat getFormat() {
    return AzureBlobStorageFormat.PARQUET;
  }

}
