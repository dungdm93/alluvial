package org.apache.iceberg.io;

import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.common.DynMethods.UnboundMethod;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.Pair;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

public class SortedPositionDeleteWriter<T> implements FileWriter<PositionDelete<T>, DeleteWriteResult> {
    private static final long RECORDS_NUM_THRESHOLD = 100_000L;
    private static final UnboundMethod closeCurrentWriterMethod = DynMethods.builder("closeCurrentWriter")
        .hiddenImpl(RollingFileWriter.class)
        .build();

    private final Multimap<String, Pair<Long, T>> deletes = Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    private final RollingPositionDeleteWriter<T> delegator;
    private long recordCount = 0L;

    public SortedPositionDeleteWriter(RollingPositionDeleteWriter<T> delegator) {
        this.delegator = delegator;
    }

    @Override
    public void write(PositionDelete<T> row) {
        var path = row.path().toString();
        var payload = Pair.of(row.pos(), row.row());

        deletes.get(path).add(payload);
        recordCount++;

        if (recordCount >= RECORDS_NUM_THRESHOLD) {
            flush();
        }
    }

    private void flush() {
        if (deletes.isEmpty()) return;
        var row = PositionDelete.<T>create();
        var paths = Lists.newArrayList(deletes.keySet());
        paths.sort(Comparators.charSequences());

        for (var path : paths) {
            var positions = (List<Pair<Long, T>>) deletes.get(path);
            positions.sort(Comparator.comparingLong(Pair<Long, T>::first));

            for (var position : positions) {
                row.set(path, position.first(), position.second());
                delegator.write(row);
            }
        }

        deletes.clear();
        recordCount = 0;

        closeCurrentWriterMethod.<Void>invoke(delegator);
        delegator.openCurrentWriter();
    }

    @Override
    public long length() {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not implement length");
    }

    @Override
    public DeleteWriteResult result() {
        return delegator.result();
    }

    @Override
    public void close() throws IOException {
        flush();
        delegator.close();
    }
}
