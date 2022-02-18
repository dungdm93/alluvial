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

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;

import java.util.List;

/**
 * A position delete writer capable of writing to multiple specs and partitions that keeps
 * delete writers for each seen spec/partition pair open until this writer is closed.
 */
public class FanoutPositionDeleteWriter<T> extends FanoutWriter<PositionDelete<T>, DeleteWriteResult> {

    private final FileWriterFactory<T> writerFactory;
    private final OutputFileFactory fileFactory;
    private final FileIO io;
    private final FileFormat fileFormat;
    private final long targetFileSizeInBytes;
    private final List<DeleteFile> deleteFiles;
    private final CharSequenceSet referencedDataFiles;

    public FanoutPositionDeleteWriter(
        FileWriterFactory<T> writerFactory, OutputFileFactory fileFactory,
        FileIO io, FileFormat fileFormat, long targetFileSizeInBytes) {
        this.writerFactory = writerFactory;
        this.fileFactory = fileFactory;
        this.io = io;
        this.fileFormat = fileFormat;
        this.targetFileSizeInBytes = targetFileSizeInBytes;
        this.deleteFiles = Lists.newArrayList();
        this.referencedDataFiles = CharSequenceSet.empty();
    }

    @Override
    protected FileWriter<PositionDelete<T>, DeleteWriteResult> newWriter(PartitionSpec spec, StructLike partition) {
        // TODO: support ORC rolling writers
        if (fileFormat == FileFormat.ORC) {
            EncryptedOutputFile outputFile = newOutputFile(fileFactory, spec, partition);
            return writerFactory.newPositionDeleteWriter(outputFile, spec, partition);
        } else {
            return new RollingPositionDeleteWriter<>(writerFactory, fileFactory, io, targetFileSizeInBytes, spec, partition);
        }
    }

    @Override
    protected void addResult(DeleteWriteResult result) {
        deleteFiles.addAll(result.deleteFiles());
        referencedDataFiles.addAll(result.referencedDataFiles());
    }

    @Override
    protected DeleteWriteResult aggregatedResult() {
        return new DeleteWriteResult(deleteFiles, referencedDataFiles);
    }
}