package io.airbyte.integrations.destination.azure_data_lake_storage_gen2;

import org.testcontainers.containers.GenericContainer;

// Azurite emulator for easier local azure storage development and testing
// https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub
public class AzureBlobStorageContainer extends GenericContainer<AzureBlobStorageContainer> {

    public AzureBlobStorageContainer() {
        super("mcr.microsoft.com/azure-storage/azurite");
    }

}
