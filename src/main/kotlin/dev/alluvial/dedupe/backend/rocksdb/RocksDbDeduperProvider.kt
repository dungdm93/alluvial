package dev.alluvial.dedupe.backend.rocksdb

import dev.alluvial.dedupe.DeduperProvider
import dev.alluvial.dedupe.RecordSerializer
import org.apache.iceberg.catalog.TableIdentifier

class RocksDbDeduperProvider<R>(private val client: RocksDbClient) : DeduperProvider<R, ByteArray> {
    override fun create(tableId: TableIdentifier, serializer: RecordSerializer<R, ByteArray>): RocksDbDeduper<R> {
        client.createTableIfNeeded(tableId.name())
        return RocksDbDeduper(tableId.name(), client, serializer)
    }
}
