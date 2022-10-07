package dev.alluvial.dedupe.backend.rocksdb

import com.google.common.base.Preconditions
import dev.alluvial.dedupe.Deduper
import dev.alluvial.dedupe.RecordSerializer
import org.apache.iceberg.relocated.com.google.common.primitives.Longs
import org.slf4j.LoggerFactory

class RocksDbDeduper<R>(
    private val table: String,
    private val client: RocksDbClient,
    private val serializer: RecordSerializer<R, ByteArray>,
) : Deduper<R> {
    companion object {
        private val logger = LoggerFactory.getLogger(RocksDbDeduper::class.java)
    }

    protected val addedEntries = mutableSetOf<R>()
    protected val deletedEntries = mutableSetOf<R>()
    private var toTruncate = false

    override fun add(record: R) {
        Preconditions.checkArgument(!toTruncate, "Cache clear operation must be committed in advance")
        addedEntries.add(record)
        deletedEntries.remove(record)
    }

    override fun remove(record: R) {
        Preconditions.checkArgument(!toTruncate, "Cache clear operation must be committed in advance")
        deletedEntries.add(record)
        addedEntries.remove(record)
    }

    override fun abort() {
        addedEntries.clear()
        deletedEntries.clear()
    }

    override fun contains(record: R): Boolean {
        if (addedEntries.contains(record)) return true
        val key = serializer.serialize(record)
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
        val addedBatch = addedEntries.map(serializer::serialize)
            .associateWithTo(mutableMapOf<ByteArray, ByteArray>()) { timestamp }
        val deletedBatch = deletedEntries.map(serializer::serialize)
        client.writeBatch(table, addedBatch, deletedBatch)
        logger.info("Added {} records to cache", addedBatch.size)
        logger.info("Deleted {} records from cache", deletedEntries.size)
        // Clear record sets
        abort()
    }
}
