package dev.alluvial.dedupe

import org.apache.iceberg.catalog.TableIdentifier

interface DeduperProvider<R, V> {
    fun create(tableId: TableIdentifier, serializer: RecordSerializer<R, V>): Deduper<R>
}
