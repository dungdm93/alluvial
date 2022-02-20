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
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public abstract class PartitioningWriterFactory<T> {
    protected final FileWriterFactory<T> writerFactory;
    protected final OutputFileFactory fileFactory;
    protected final FileIO io;
    protected final FileFormat fileFormat;
    protected final long targetFileSizeInBytes;

    public PartitioningWriterFactory(
        FileWriterFactory<T> writerFactory, OutputFileFactory fileFactory,
        FileIO io, FileFormat fileFormat, long targetFileSizeInBytes) {
        this.writerFactory = writerFactory;
        this.fileFactory = fileFactory;
        this.io = io;
        this.fileFormat = fileFormat;
        this.targetFileSizeInBytes = targetFileSizeInBytes;
    }

    public abstract PartitioningWriter<T, DataWriteResult> newDataWriter();

    public abstract PartitioningWriter<T, DeleteWriteResult> newEqualityDeleteWriter();

    public abstract PartitioningWriter<PositionDelete<T>, DeleteWriteResult> newPositionDeleteWriter();

    public static <T> Builder<T> builder(FileWriterFactory<T> writerFactory) {
        return new Builder<>(writerFactory);
    }

    public static class Builder<T> {
        private FileWriterFactory<T> writerFactory = null;
        private OutputFileFactory fileFactory = null;
        private FileIO io = null;
        private FileFormat fileFormat = null;
        private long targetFileSizeInBytes = 0;

        public Builder(FileWriterFactory<T> writerFactory) {
            this.writerFactory = writerFactory;
        }

        private void checkArguments() {
            Preconditions.checkArgument(writerFactory != null, "writerFactory is required non-null");
            Preconditions.checkArgument(fileFactory != null, "fileFactory is required non-null");
            Preconditions.checkArgument(io != null, "io is required non-null");
        }

        public PartitioningWriterFactory<T> buildForClusteredPartition() {
            checkArguments();
            return new ClusteredWriterFactory<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }

        public PartitioningWriterFactory<T> buildForFanoutPartition() {
            checkArguments();
            return new FanoutWriterFactory<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }

        public Builder<T> fileFactory(OutputFileFactory fileFactory) {
            this.fileFactory = fileFactory;
            return this;
        }

        public Builder<T> io(FileIO io) {
            this.io = io;
            return this;
        }

        public Builder<T> fileFormat(FileFormat fileFormat) {
            this.fileFormat = fileFormat;
            return this;
        }

        public Builder<T> targetFileSizeInBytes(long targetFileSizeInBytes) {
            this.targetFileSizeInBytes = targetFileSizeInBytes;
            return this;
        }
    }

    protected static class ClusteredWriterFactory<T> extends PartitioningWriterFactory<T> {
        public ClusteredWriterFactory(
            FileWriterFactory<T> writerFactory, OutputFileFactory fileFactory,
            FileIO io, FileFormat fileFormat, long targetFileSizeInBytes) {
            super(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }

        @Override
        public PartitioningWriter<T, DataWriteResult> newDataWriter() {
            return new ClusteredDataWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }

        @Override
        public PartitioningWriter<T, DeleteWriteResult> newEqualityDeleteWriter() {
            return new ClusteredEqualityDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }

        @Override
        public PartitioningWriter<PositionDelete<T>, DeleteWriteResult> newPositionDeleteWriter() {
            return new ClusteredPositionDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }
    }

    protected static class FanoutWriterFactory<T> extends PartitioningWriterFactory<T> {
        public FanoutWriterFactory(
            FileWriterFactory<T> writerFactory, OutputFileFactory fileFactory,
            FileIO io, FileFormat fileFormat, long targetFileSizeInBytes) {
            super(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }

        @Override
        public PartitioningWriter<T, DataWriteResult> newDataWriter() {
            return new FanoutDataWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }

        @Override
        public PartitioningWriter<T, DeleteWriteResult> newEqualityDeleteWriter() {
            return new FanoutEqualityDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }

        @Override
        public PartitioningWriter<PositionDelete<T>, DeleteWriteResult> newPositionDeleteWriter() {
            return new FanoutPositionDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
        }
    }
}
