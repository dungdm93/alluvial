package dev.alluvial.sink.iceberg.io;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

public interface TrackedPartitioningWriter<T, R> extends PartitioningWriter<T, R> {
    PathOffset trackedWrite(T row, PartitionSpec spec, StructLike partition);
}
