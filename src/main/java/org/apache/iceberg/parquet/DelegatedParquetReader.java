package org.apache.iceberg.parquet;

import org.apache.parquet.column.page.PageReadStore;

import java.util.List;

public abstract class DelegatedParquetReader<T, D> implements ParquetValueReader<T> {

    protected ParquetValueReader<D> delegator;

    public DelegatedParquetReader(ParquetValueReader<D> delegator) {
        this.delegator = delegator;
    }

    @Override
    public TripleIterator<?> column() {
        return delegator.column();
    }

    @Override
    public List<TripleIterator<?>> columns() {
        return delegator.columns();
    }

    @Override
    public void setPageSource(PageReadStore pageStore, long rowPosition) {
        delegator.setPageSource(pageStore, rowPosition);
    }
}
