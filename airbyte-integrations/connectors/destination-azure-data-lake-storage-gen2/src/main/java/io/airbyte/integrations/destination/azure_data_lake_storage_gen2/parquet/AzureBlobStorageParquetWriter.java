/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_data_lake_storage_gen2.parquet;

import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.AzureDataLakeStorageGen2DestinationConfig;
import io.airbyte.integrations.destination.azure_data_lake_storage_gen2.writer.AzureBlobStorageWriter;
import io.airbyte.integrations.destination.s3.S3DestinationConstants;
import io.airbyte.integrations.destination.s3.S3Format;
import io.airbyte.integrations.destination.s3.avro.AvroRecordFactory;
import io.airbyte.integrations.destination.s3.parquet.S3ParquetFormatConfig;
import io.airbyte.integrations.destination.s3.template.S3FilenameTemplateParameterObject;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.UUID;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class AzureBlobStorageParquetWriter implements AzureBlobStorageWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobStorageParquetWriter.class);

    private final ParquetWriter<Record> parquetWriter;
    private final AvroRecordFactory avroRecordFactory;
    private final Schema schema;
    private final String outputFilename;
    // object key = <path>/<output-filename>
    private static final String DEFAULT_SUFFIX = "_0";
    private final AirbyteStream stream;

    public AzureBlobStorageParquetWriter(final AzureDataLakeStorageGen2DestinationConfig config,
                                         final Configuration hadoopConfig,
                                         final ConfiguredAirbyteStream configuredStream,
                                         final Timestamp uploadTimestamp,
                                         final Schema schema,
                                         final JsonAvroConverter converter)
            throws URISyntaxException, IOException {

        outputFilename = determineOutputFilename(S3FilenameTemplateParameterObject
                .builder()
                .s3Format(S3Format.PARQUET)
                .timestamp(uploadTimestamp)
                .fileExtension(S3Format.PARQUET.getFileExtension())
                .fileNamePattern(null)
                .build());
        this.stream = configuredStream.getStream();

        final Path path = new Path(outputFilename);
        final S3ParquetFormatConfig formatConfig = (S3ParquetFormatConfig) config.getFormatConfig();

        this.parquetWriter = AvroParquetWriter.<Record>builder(HadoopOutputFile.fromPath(path, hadoopConfig))
                .withSchema(schema)
                .withCompressionCodec(formatConfig.getCompressionCodec())
                .withRowGroupSize(formatConfig.getBlockSize())
                .withMaxPaddingSize(formatConfig.getMaxPaddingSize())
                .withPageSize(formatConfig.getPageSize())
                .withDictionaryPageSize(formatConfig.getDictionaryPageSize())
                .withDictionaryEncoding(formatConfig.isDictionaryEncoding())
                .build();
        this.avroRecordFactory = new AvroRecordFactory(schema, converter);
        this.schema = schema;
    }


    public Schema getSchema() {
        return schema;
    }

    /**
     * The file path includes prefix and filename, but does not include the bucket name.
     */

    public String getOutputFilename() {
        return outputFilename;
    }

    public static String determineOutputFilename(final S3FilenameTemplateParameterObject parameterObject)
            throws IOException {
        return isNotBlank(parameterObject.getFileNamePattern()) ? getOutputFilename(parameterObject) : getDefaultOutputFilename(parameterObject);
    }

    private static String getOutputFilename(final S3FilenameTemplateParameterObject parameterObject) {

        return parameterObject.getObjectPath();
    }

    private static String getDefaultOutputFilename(final S3FilenameTemplateParameterObject parameterObject) {
        final DateFormat formatter = new SimpleDateFormat(S3DestinationConstants.YYYY_MM_DD_FORMAT_STRING);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return String.format(
                "%s_%d%s.%s",
                formatter.format(parameterObject.getTimestamp()),
                parameterObject.getTimestamp().getTime(),
                null == parameterObject.getCustomSuffix() ? DEFAULT_SUFFIX : parameterObject.getCustomSuffix(),
                parameterObject.getS3Format().getFileExtension());
    }

    @Override
    public void write(final UUID id, final AirbyteRecordMessage recordMessage) throws IOException {
        parquetWriter.write(avroRecordFactory.getAvroRecord(id, recordMessage));
    }

    /**
     * Log and close the write.
     */
    @Override
    public void close(final boolean hasFailed) throws IOException {
        if (hasFailed) {
            LOGGER.warn("Failure detected. Aborting upload of stream '{}'...", stream.getName());
            closeWhenFail();
            LOGGER.warn("Upload of stream '{}' aborted.", stream.getName());
        } else {
            LOGGER.info("Uploading remaining data for stream '{}'.", stream.getName());
            closeWhenSucceed();
            LOGGER.info("Upload completed for stream '{}'.", stream.getName());
        }
    }

    protected void closeWhenSucceed() throws IOException {
        parquetWriter.close();
    }

    protected void closeWhenFail() throws IOException {
        parquetWriter.close();
    }

}
