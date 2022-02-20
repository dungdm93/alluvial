/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package dev.alluvial.backport.iceberg.io;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.BackportAvro;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.BackportORC;
import org.apache.iceberg.parquet.BackportParquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * A base writer factory to be extended by query engine integrations.
 */
public abstract class BaseFileWriterFactory<T> implements FileWriterFactory<T> {
    private final Table table;
    private final FileFormat dataFileFormat;
    private final Schema dataSchema;
    private final SortOrder dataSortOrder;
    private final FileFormat deleteFileFormat;
    private final int[] equalityFieldIds;
    private final Schema equalityDeleteRowSchema;
    private final SortOrder equalityDeleteSortOrder;
    private final Schema positionDeleteRowSchema;

    protected BaseFileWriterFactory(Table table, FileFormat dataFileFormat, Schema dataSchema,
                                    SortOrder dataSortOrder, FileFormat deleteFileFormat,
                                    int[] equalityFieldIds, Schema equalityDeleteRowSchema,
                                    SortOrder equalityDeleteSortOrder, Schema positionDeleteRowSchema) {
        this.table = table;
        this.dataFileFormat = dataFileFormat;
        this.dataSchema = dataSchema;
        this.dataSortOrder = dataSortOrder;
        this.deleteFileFormat = deleteFileFormat;
        this.equalityFieldIds = equalityFieldIds;
        this.equalityDeleteRowSchema = equalityDeleteRowSchema;
        this.equalityDeleteSortOrder = equalityDeleteSortOrder;
        this.positionDeleteRowSchema = positionDeleteRowSchema;
    }

    protected abstract void configureDataWrite(BackportAvro.DataWriteBuilder builder);

    protected abstract void configureEqualityDelete(BackportAvro.DeleteWriteBuilder builder);

    protected abstract void configurePositionDelete(BackportAvro.DeleteWriteBuilder builder);

    protected abstract void configureDataWrite(BackportParquet.DataWriteBuilder builder);

    protected abstract void configureEqualityDelete(BackportParquet.DeleteWriteBuilder builder);

    protected abstract void configurePositionDelete(BackportParquet.DeleteWriteBuilder builder);

    protected abstract void configureDataWrite(BackportORC.DataWriteBuilder builder);

    protected abstract void configureEqualityDelete(BackportORC.DeleteWriteBuilder builder);

    protected abstract void configurePositionDelete(BackportORC.DeleteWriteBuilder builder);

    @Override
    public DataWriter<T> newDataWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
        OutputFile outputFile = file.encryptingOutputFile();
        EncryptionKeyMetadata keyMetadata = file.keyMetadata();
        Map<String, String> properties = table.properties();
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);

        try {
            switch (dataFileFormat) {
                case AVRO:
                    BackportAvro.DataWriteBuilder avroBuilder = BackportAvro.writeData(outputFile)
                        .schema(dataSchema)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .withSortOrder(dataSortOrder)
                        .overwrite();

                    configureDataWrite(avroBuilder);

                    return avroBuilder.build();

                case PARQUET:
                    BackportParquet.DataWriteBuilder parquetBuilder = BackportParquet.writeData(outputFile)
                        .schema(dataSchema)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .withSortOrder(dataSortOrder)
                        .overwrite();

                    configureDataWrite(parquetBuilder);

                    return parquetBuilder.build();

                case ORC:
                    BackportORC.DataWriteBuilder orcBuilder = BackportORC.writeData(outputFile)
                        .schema(dataSchema)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .withSortOrder(dataSortOrder)
                        .overwrite();

                    configureDataWrite(orcBuilder);

                    return orcBuilder.build();

                default:
                    throw new UnsupportedOperationException("Unsupported data file format: " + dataFileFormat);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public EqualityDeleteWriter<T> newEqualityDeleteWriter(EncryptedOutputFile file, PartitionSpec spec,
                                                           StructLike partition) {
        OutputFile outputFile = file.encryptingOutputFile();
        EncryptionKeyMetadata keyMetadata = file.keyMetadata();
        Map<String, String> properties = table.properties();
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);

        try {
            switch (deleteFileFormat) {
                case AVRO:
                    BackportAvro.DeleteWriteBuilder avroBuilder = BackportAvro.writeDeletes(outputFile)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .rowSchema(equalityDeleteRowSchema)
                        .equalityFieldIds(equalityFieldIds)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .withSortOrder(equalityDeleteSortOrder)
                        .overwrite();

                    configureEqualityDelete(avroBuilder);

                    return avroBuilder.buildEqualityWriter();

                case PARQUET:
                    BackportParquet.DeleteWriteBuilder parquetBuilder = BackportParquet.writeDeletes(outputFile)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .rowSchema(equalityDeleteRowSchema)
                        .equalityFieldIds(equalityFieldIds)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .withSortOrder(equalityDeleteSortOrder)
                        .overwrite();

                    configureEqualityDelete(parquetBuilder);

                    return parquetBuilder.buildEqualityWriter();

                case ORC:
                    BackportORC.DeleteWriteBuilder orcBuilder = BackportORC.writeDeletes(outputFile)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .rowSchema(equalityDeleteRowSchema)
                        .equalityFieldIds(equalityFieldIds)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .withSortOrder(equalityDeleteSortOrder)
                        .overwrite();

                    configureEqualityDelete(orcBuilder);

                    return orcBuilder.buildEqualityWriter();

                default:
                    throw new UnsupportedOperationException("Unsupported format for equality deletes: " + deleteFileFormat);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create new equality delete writer", e);
        }
    }

    @Override
    public PositionDeleteWriter<T> newPositionDeleteWriter(EncryptedOutputFile file, PartitionSpec spec,
                                                           StructLike partition) {
        OutputFile outputFile = file.encryptingOutputFile();
        EncryptionKeyMetadata keyMetadata = file.keyMetadata();
        Map<String, String> properties = table.properties();
        MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);

        try {
            switch (deleteFileFormat) {
                case AVRO:
                    BackportAvro.DeleteWriteBuilder avroBuilder = BackportAvro.writeDeletes(outputFile)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .rowSchema(positionDeleteRowSchema)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .overwrite();

                    configurePositionDelete(avroBuilder);

                    return avroBuilder.buildPositionWriter();

                case PARQUET:
                    BackportParquet.DeleteWriteBuilder parquetBuilder = BackportParquet.writeDeletes(outputFile)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .rowSchema(positionDeleteRowSchema)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .overwrite();

                    configurePositionDelete(parquetBuilder);

                    return parquetBuilder.buildPositionWriter();

                case ORC:
                    BackportORC.DeleteWriteBuilder orcBuilder = BackportORC.writeDeletes(outputFile)
                        .setAll(properties)
                        .metricsConfig(metricsConfig)
                        .rowSchema(positionDeleteRowSchema)
                        .withSpec(spec)
                        .withPartition(partition)
                        .withKeyMetadata(keyMetadata)
                        .overwrite();

                    configurePositionDelete(orcBuilder);

                    return orcBuilder.buildPositionWriter();

                default:
                    throw new UnsupportedOperationException("Unsupported format for position deletes: " + deleteFileFormat);
            }

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create new position delete writer", e);
        }
    }

    protected Schema dataSchema() {
        return dataSchema;
    }

    protected Schema equalityDeleteRowSchema() {
        return equalityDeleteRowSchema;
    }

    protected Schema positionDeleteRowSchema() {
        return positionDeleteRowSchema;
    }
}
