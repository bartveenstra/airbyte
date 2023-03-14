/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.*;
import com.azure.storage.file.datalake.models.PathItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureDataLakeStorageGen2ConnectionChecker {

  private static final String TEST_BLOB_NAME_PREFIX = "testConnectionBlob";

  private final DataLakeServiceClient datalakeClient; // aka schema in SQL DBs
  private final AzureDataLakeStorageGen2DestinationConfig azureDataLakeStorageGen2DestinationConfig;
  private DataLakeDirectoryClient directoryClient; // aka "SQL Table"

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureDataLakeStorageGen2ConnectionChecker.class);
  private DataLakeDirectoryClient basePathClient;
  private String testFile;

  public AzureDataLakeStorageGen2ConnectionChecker(final AzureDataLakeStorageGen2DestinationConfig azureDataLakeStorageGen2DestinationConfig) {

    DataLakeServiceClientBuilder dataLakeClientBuilder = AzureDataLakeStorageGen2DestinationConfig.createDataLakeClientBuilder(azureDataLakeStorageGen2DestinationConfig);
    this.datalakeClient = dataLakeClientBuilder.buildClient();
    this.azureDataLakeStorageGen2DestinationConfig = azureDataLakeStorageGen2DestinationConfig;
  }

  /*
   * This a kinda test method that is used in CHECK operation to make sure all works fine with the
   * current config
   */
  public void attemptWriteAndDelete() {
    this.testFile = "test_" + System.currentTimeMillis();

    initTestContainerAndDirectory();
    writeUsingAppendBlock("Some test data");
    listBlobsInContainer().forEach(pathItem -> LOGGER.info("File name: {}, Snapshot: {}", pathItem.getName(), pathItem.getCreationTime()));
    deleteBlob();
  }

  private void initTestContainerAndDirectory() {

    DataLakeFileSystemClient fileSystemClient = datalakeClient.getFileSystemClient(azureDataLakeStorageGen2DestinationConfig.getContainerName());

    if (!fileSystemClient.exists()) {
      fileSystemClient = datalakeClient.createFileSystem(azureDataLakeStorageGen2DestinationConfig.getContainerName());
      LOGGER.info("fileSystemClient created");
    } else {
      LOGGER.info("fileSystemClient already exists");
    }


     this.directoryClient = fileSystemClient.getDirectoryClient(azureDataLakeStorageGen2DestinationConfig.getBasePath());
    if (!this.directoryClient.exists()) {
      LOGGER.info("directoryClient created");
      this.directoryClient = fileSystemClient.createDirectory(azureDataLakeStorageGen2DestinationConfig.getBasePath());
    } else {
      LOGGER.info("directoryClient already exists");
    }


  }

  /*
   * This options may be used to write and flush right away. Note: Azure SDK fails for empty lines,
   * but those are not supposed to be written here
   */
  public void writeUsingAppendBlock(final String data) {
    LOGGER.info("Writing test data to Azure Blob storage: {}", data);
    DataLakeFileClient file = this.directoryClient.createFile(testFile);
    file.upload(BinaryData.fromBytes(data.getBytes()), true);
  }

  /*
   * List the blob(s) in our container.
   */
  public PagedIterable<PathItem> listBlobsInContainer() {
    return this.directoryClient.listPaths();
  }

  /*
   * Delete the blob we created earlier.
   */
  public void deleteBlob() {
    LOGGER.info("Deleting file: {}", testFile);
    directoryClient.deleteFileIfExists(testFile);
  }


}
