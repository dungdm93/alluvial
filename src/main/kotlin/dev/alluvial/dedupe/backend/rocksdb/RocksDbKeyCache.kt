package dev.alluvial.dedupe.backend.rocksdb

import com.google.common.base.Preconditions
import dev.alluvial.dedupe.KeyCache
import dev.alluvial.sink.iceberg.type.KafkaStruct
import org.apache.iceberg.relocated.com.google.common.primitives.Longs
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import java.lang.StringBuilder

class RocksDbKeyCache(
    private val table: String,
    private val backend: RocksDbBackend,
    private val identifiers: List<String>,
    private val delimiter: String = "-"
) : KeyCache<SinkRecord, ByteArray, ByteArray>(table, backend) {
    companion object {
        private val logger = LoggerFactory.getLogger(RocksDbKeyCache::class.java)
    }

    override fun hasKey(record: SinkRecord): Boolean {
        if (addedEntries.contains(record)) return true
        val key = serializeKey(record)
        return backend.hasKey(table, key)
    }

    override fun commit() {
        val timestamp = Longs.toByteArray(System.currentTimeMillis())
        val addedBatch = addedEntries.map(this::serializeKey)
            .associateWithTo(mutableMapOf<ByteArray, ByteArray>()) { timestamp }
        val deletedBatch = deletedEntries.map(this::serializeKey)
        backend.writeBatch(table, addedBatch, deletedBatch)
        logger.info("Added {} records to cache", addedBatch.size)
        logger.info("Deleted {} records from cache", deletedEntries.size)
        abort()
    }

    /**
     * Transform record key to string then serialize to bytes
     */
    override fun serializeKey(record: SinkRecord): ByteArray {
        Preconditions.checkArgument(record.key() != null, "Record key must be non null")
        val keyStruct = record.key() as KafkaStruct
        val builder = StringBuilder()
        identifiers.forEach { idName ->
            val idValue = keyStruct.get(idName)
            if (builder.isNotEmpty()) {
                builder.append(delimiter)
            }
            builder.append(idValue.toString())
        }
        return builder.toString().encodeToByteArray()
    }
}
