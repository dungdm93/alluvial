package org.apache.iceberg.io;

import dev.alluvial.sink.iceberg.io.PathOffset;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public interface TrackedFileWriter<T, R> extends FileWriter<T, R> {
    static <T, R> TrackedFileWriter<T, R> wrap(FileWriter<T, R> fileWriter) {
        Preconditions.checkNotNull(fileWriter, "fileWriter cannot be null");

        if (fileWriter instanceof TrackedFileWriter) {
            return (TrackedFileWriter<T, R>) fileWriter;
        } else if (fileWriter instanceof RollingFileWriter) {
            return new RollingTrackedFileWriter<>((RollingFileWriter<T, ?, R>) fileWriter);
        } else {
            return new NormalTrackedFileWriter<>(fileWriter);
        }
    }

    PathOffset trackedWrite(T row);
}
