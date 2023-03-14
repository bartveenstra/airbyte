# Azure Data Lake Storage Gen 2 Test Configuration

In order to test the Azure Data Lake Storage Gen 2 destination, you need a Microsoft account.

## Community Contributor

As a community contributor, you will need access to Azure to run the integration tests.

- Create an AzureDataLakeStorageGen2 account for testing. Check if it works under https://portal.azure.com/ -> "Storage explorer (preview)".
- Get your `sas_token` or `storage_account_key` and `azure_data_lake_storage_gen2_storage_account_name` that can read and write to the Azure Container.
- Paste the accountName and key information into the config files under [`./sample_secrets`](./sample_secrets).
- Rename the directory from `sample_secrets` to `secrets`.
- Feel free to modify the config files with different settings in the acceptance test file (e.g. `AzureDataLakeStorageGen2ParquetDestinationAcceptanceTest.java`, method `getFormatConfig`), as long as they follow the schema defined in [spec.json](src/main/resources/spec.json).

## Airbyte Employee
- Access the `Azure Data Lake Storage Gen 2 Account` secrets on Last Pass.
- Replace the `config.json` under `sample_secrets`.
- Rename the directory from `sample_secrets` to `secrets`.

## Add New Output Format
- Add a new enum in `AzureDataLakeStorageGen2Format'.
- Modify `spec.json` to specify the configuration of this new format.
- Update `AzureDataLakeStorageGen2FormatConfigs` to be able to construct a config for this new format.
- Create a new package under `io.airbyte.integrations.destination.azure_data_lake_storage_gen2`.
- Implement a new `AzureDataLakeStorageGen2Writer`. The implementation can extend `BaseAzureDataLakeStorageGen2Writer`.
- Write an acceptance test for the new output format. The test can extend `AzureDataLakeStorageGen2DestinationAcceptanceTest`.


