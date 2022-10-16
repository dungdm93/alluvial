package dev.alluvial.dedupe.backend.rocksdb

import com.google.common.base.Preconditions
import dev.alluvial.dedupe.Deduper
import dev.alluvial.dedupe.RecordSerializer
import org.apache.iceberg.relocated.com.google.common.primitives.Longs
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory

class RocksDbDeduper<R>(
    private val table: String,
    private val client: RocksDbClient,
    private val serializer: RecordSerializer<R, ByteArray>,
) : Deduper<R> {
    companion object {
        private val logger = LoggerFactory.getLogger(RocksDbDeduper::class.java)
    }

    protected val addedEntries = mutableSetOf<String>()
    protected val deletedEntries = mutableSetOf<String>()
    private var toTruncate = false

    override fun add(record: R) {
        Preconditions.checkArgument(!toTruncate, "Cache clear operation must be committed in advance")
        val key = serializer.serialize(record).decodeToString()
        addedEntries.add(key)
        deletedEntries.remove(key)
    }

    override fun remove(record: R) {
        Preconditions.checkArgument(!toTruncate, "Cache clear operation must be committed in advance")
        val key = serializer.serialize(record).decodeToString()
        deletedEntries.add(key)
        addedEntries.remove(key)
    }

    override fun abort() {
        addedEntries.clear()
        deletedEntries.clear()
    }

    override fun contains(record: R): Boolean {
        val key = serializer.serialize(record)
        if (addedEntries.contains(key.decodeToString())) return true
        return client.hasKey(table, key)
    }

    override fun clear() {
        Preconditions.checkArgument(
            addedEntries.isEmpty() && deletedEntries.isEmpty(),
            "Record in progress must be committed in advanced"
        )
        toTruncate = true
    }

    override fun commit() {
        if (toTruncate) {
            client.truncate(table)
            toTruncate = false
        }

        val timestamp = Longs.toByteArray(System.currentTimeMillis())
        val addedBatch = addedEntries.map(String::encodeToByteArray)
            .associateWithTo(mutableMapOf<ByteArray, ByteArray>()) { timestamp }
        val deletedBatch = deletedEntries.map(String::encodeToByteArray)
        client.writeBatch(table, addedBatch, deletedBatch)
        logger.info("Added {} records to cache", addedBatch.size)
        logger.info("Deleted {} records from cache", deletedEntries.size)

        client.flush(table)
        logger.info("Flushed memtable of table {}", table)
        // Clear record sets
        abort()
    }
}
