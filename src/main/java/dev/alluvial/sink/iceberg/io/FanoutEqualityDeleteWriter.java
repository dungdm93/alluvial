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
package dev.alluvial.sink.iceberg.io;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingEqualityDeleteWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import java.util.List;

/**
 * An equality delete writer capable of writing to multiple specs and partitions that keeps
 * delete writers for each seen spec/partition pair open until this writer is closed.
 */
public class FanoutEqualityDeleteWriter<T>
    extends FanoutWriter<T, RollingEqualityDeleteWriter<T>, DeleteWriteResult>
    implements TrackedPartitioningWriter<T, DeleteWriteResult> {

    private final FileWriterFactory<T> writerFactory;
    private final OutputFileFactory fileFactory;
    private final FileIO io;
    private final long targetFileSizeInBytes;
    private final List<DeleteFile> deleteFiles;

    public FanoutEqualityDeleteWriter(FileWriterFactory<T> writerFactory, OutputFileFactory fileFactory,
                                      FileIO io, long targetFileSizeInBytes) {
        this.writerFactory = writerFactory;
        this.fileFactory = fileFactory;
        this.io = io;
        this.targetFileSizeInBytes = targetFileSizeInBytes;
        this.deleteFiles = Lists.newArrayList();
    }

    @Override
    public PathOffset trackedWrite(T row, PartitionSpec spec, StructLike partition) {
        write(row, spec, partition);

        @SuppressWarnings("resource")
        var writer = writer(spec, partition);
        var path = writer.currentFilePath();
        var offset = writer.currentFileRows() - 1;
        return PathOffset.of(path, offset);
    }

    @Override
    protected RollingEqualityDeleteWriter<T> newWriter(PartitionSpec spec, StructLike partition) {
        return new RollingEqualityDeleteWriter<>(writerFactory, fileFactory, io, targetFileSizeInBytes, spec, partition);
    }

    @Override
    protected void addResult(DeleteWriteResult result) {
        Preconditions.checkArgument(!result.referencesDataFiles(), "Equality deletes cannot reference data files");
        deleteFiles.addAll(result.deleteFiles());
    }

    @Override
    protected DeleteWriteResult aggregatedResult() {
        return new DeleteWriteResult(deleteFiles);
    }
}
