package dev.alluvial.stream.debezium

import dev.alluvial.sink.iceberg.type.IcebergTable
import dev.alluvial.source.kafka.op
import dev.alluvial.source.kafka.source
import org.apache.iceberg.BROKER_OFFSETS_PROP
import org.apache.iceberg.SOURCE_TIMESTAMP_PROP
import org.apache.iceberg.SOURCE_WAL_POSITION_PROP
import org.apache.iceberg.brokerOffsets
import org.apache.iceberg.mapper
import org.apache.iceberg.sourceTimestampMillis
import org.apache.iceberg.sourceWALPosition
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory

class RecordTracker private constructor(
    private val table: IcebergTable,
    private val consumedBrokerOffsets: MutableMap<Int, Long>,
    private var lastSourceTimestamp: Long,
    private var lastSourceWALPosition: WALPosition,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(RecordTracker::class.java)

        fun create(connector: String, table: IcebergTable): RecordTracker {
            val currentSnapshot = table.currentSnapshot()

            if (currentSnapshot == null) {
                val offsets = mutableMapOf<Int, Long>()
                val timestamp = Long.MIN_VALUE
                val walPosition = defaultWALPositionFor(connector)
                return RecordTracker(table, offsets, timestamp, walPosition)
            }

            val offsets = HashMap(currentSnapshot.brokerOffsets())
            val timestamp = currentSnapshot.sourceTimestampMillis()!!
            val walPosition = currentSnapshot.sourceWALPosition()!!
            return RecordTracker(table, offsets, timestamp, walPosition)
        }

        private fun defaultWALPositionFor(connector: String): WALPosition {
            return when (connector) {
                "mysql" -> MySqlWALPosition(-1, -1)
                "postgresql" -> PostgresWALPosition(-1)
                else -> throw UnsupportedOperationException("connector $connector is not supported yet")
            }
        }
    }

    /**
     * @return broker offsets of last snapshot or `emptyMap`
     * if table doesn't have any snapshot.
     */
    fun committedBrokerOffsets(): Map<Int, Long> {
        return table.currentSnapshot()?.brokerOffsets() ?: emptyMap()
    }

    /**
     * @return timestamp in millisecond of last record committed or null
     * if table doesn't have any snapshot.
     */
    fun committedSourceTimestamp(): Long? {
        return table.currentSnapshot()?.sourceTimestampMillis()
    }

    /**
     * @return WALPosition of last record committed or `null`
     * if table doesn't have any snapshot.
     */
    fun committedSourceWALPosition(): WALPosition? {
        return table.currentSnapshot()?.sourceWALPosition()
    }

    fun consumedBrokerOffsets(): Map<Int, Long> = consumedBrokerOffsets
    fun lastSourceTimestamp() = lastSourceTimestamp
    fun lastSourceWALPosition() = lastSourceWALPosition

    /**
     * @return `true` if given record possibly be duplicated,
     *         `false` if it is definitely NOT
     */
    fun maybeDuplicate(record: SinkRecord): Boolean {
        val source = record.source() ?: return false
        val sourceTimestamp = source.getInt64("ts_ms")
        return lastSourceTimestamp > sourceTimestamp
            || lastSourceWALPosition >= source
    }

    fun update(record: SinkRecord) {
        consumedBrokerOffsets[record.kafkaPartition()] = record.kafkaOffset() + 1
        val source = record.source() ?: return // ignore tombstone records
        var ordered = true

        val sourceTimestamp = source.getInt64("ts_ms")
        if (lastSourceTimestamp <= sourceTimestamp)
            lastSourceTimestamp = sourceTimestamp
        else ordered = false

        val sourceWALPosition = lastSourceWALPosition.fromSource(source) ?: return
        if (lastSourceWALPosition < sourceWALPosition)
            lastSourceWALPosition = sourceWALPosition
        else ordered = ordered && lastSourceWALPosition == sourceWALPosition && "r" == record.op()

        if (!ordered)
            logger.warn(
                "Receive un-ordered message at partition {}, offset {}\n" +
                    "\tlastSourceTimestamp={}, lastWALPosition={}\n" +
                    "\tsourceTimestamp={}, WALPosition={}",
                record.kafkaPartition(), record.kafkaOffset(),
                lastSourceTimestamp, lastSourceWALPosition,
                sourceTimestamp, sourceWALPosition,
            )
    }

    fun buildSummary(): Map<String, String> {
        return mapOf(
            SOURCE_TIMESTAMP_PROP to lastSourceTimestamp.toString(),
            SOURCE_WAL_POSITION_PROP to mapper.writeValueAsString(lastSourceWALPosition),
            BROKER_OFFSETS_PROP to mapper.writeValueAsString(consumedBrokerOffsets)
        )
    }
}
