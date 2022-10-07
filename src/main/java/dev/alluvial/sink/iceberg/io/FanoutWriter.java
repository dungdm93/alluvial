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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.io.HelpersKt;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeMap;

import java.io.IOException;
import java.util.Map;

/**
 * A writer capable of writing to multiple specs and partitions that keeps files for each
 * seen spec/partition pair open until this writer is closed.
 * <p>
 * As opposed to {@link ClusteredWriter}, this writer does not require the incoming records
 * to be clustered by partition spec and partition as all files are kept open. As a consequence,
 * this writer may potentially consume substantially more memory compared to {@link ClusteredWriter}.
 * Use this writer only when clustering by spec/partition is not possible (e.g. streaming).
 */
abstract class FanoutWriter<T, W extends FileWriter<T, R>, R> implements PartitioningWriter<T, R> {

    private final Map<Integer, StructLikeMap<W>> writers = Maps.newHashMap();
    private boolean closed = false;

    protected abstract W newWriter(PartitionSpec spec, StructLike partition);

    protected abstract void addResult(R result);

    protected abstract R aggregatedResult();

    @Override
    public void write(T row, PartitionSpec spec, StructLike partition) {
        W writer = writer(spec, partition);
        writer.write(row);
    }

    protected W writer(PartitionSpec spec, StructLike partition) {
        Map<StructLike, W> specWriters =
            writers.computeIfAbsent(spec.specId(), id -> StructLikeMap.create(spec.partitionType()));
        W writer = specWriters.get(partition);

        if (writer == null) {
            // copy the partition key as the key object may be reused
            StructLike copiedPartition = HelpersKt.copy(partition);
            writer = newWriter(spec, copiedPartition);
            specWriters.put(copiedPartition, writer);
        }

        return writer;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closeWriters();
            this.closed = true;
        }
    }

    private void closeWriters() throws IOException {
        for (Map<StructLike, W> specWriters : writers.values()) {
            for (W writer : specWriters.values()) {
                writer.close();
                addResult(writer.result());
            }

            specWriters.clear();
        }

        writers.clear();
    }

    @Override
    public final R result() {
        Preconditions.checkState(closed, "Cannot get result from unclosed writer");
        return aggregatedResult();
    }

    protected EncryptedOutputFile newOutputFile(OutputFileFactory fileFactory, PartitionSpec spec, StructLike partition) {
        Preconditions.checkArgument(spec.isUnpartitioned() || partition != null,
            "Partition must not be null when creating output file for partitioned spec");
        return partition == null ? fileFactory.newOutputFile() : fileFactory.newOutputFile(spec, partition);
    }
}
