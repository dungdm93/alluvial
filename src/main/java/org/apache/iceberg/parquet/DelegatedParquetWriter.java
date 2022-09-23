package org.apache.iceberg.parquet;

import org.apache.iceberg.FieldMetrics;
import org.apache.parquet.column.ColumnWriteStore;

import java.util.List;
import java.util.stream.Stream;

public abstract class DelegatedParquetWriter<T, D> implements ParquetValueWriter<T> {

    protected ParquetValueWriter<D> delegator;

    protected DelegatedParquetWriter(ParquetValueWriter<D> delegator) {
        this.delegator = delegator;
    }

    @Override
    public List<TripleWriter<?>> columns() {
        return delegator.columns();
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
        delegator.setColumnStore(columnStore);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
        return delegator.metrics();
    }
}
