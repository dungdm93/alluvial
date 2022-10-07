package dev.alluvial.dedupe.backend.rocksdb

import dev.alluvial.dedupe.RecordSerializer

interface RocksDbRecordSerializer<R> : RecordSerializer<R, ByteArray> {
    override fun serialize(record: R): ByteArray
}
