package dev.alluvial.stream.debezium

import dev.alluvial.api.Streamlet
import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.api.StreamletId
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaTopicInlet
import dev.alluvial.utils.SystemTime
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.Objects
import kotlin.time.Duration.Companion.minutes

class DebeziumStreamlet(
    override val id: StreamletId,
    val inlet: KafkaTopicInlet,
    val outlet: IcebergTableOutlet,
) : Streamlet {
    companion object {
        private val logger = LoggerFactory.getLogger(DebeziumStreamlet::class.java)
    }

    private val positions = mutableMapOf<Int, Long>()
    private var hashedSchema: Int? = null
    private val time = SystemTime
    var commitBatchSize = 1000
    var commitTimespanMs = 10.minutes.inWholeMilliseconds

    @Volatile
    override var status = CREATED

    override fun run() = withMDC {
        if (status != CREATED) resume()
        status = RUNNING
        logger.info("Streamlet {} is running", id)
        ensurePosition()
        while (
            inlet.currentLag() >= commitBatchSize ||
            (outlet.committedTimestamp() ?: Long.MIN_VALUE) < time.millis() - commitTimespanMs
        ) {
            captureChanges(commitBatchSize)
        }
        pause()
        status = SUSPENDED
    }

    override fun pause() {
        inlet.pause()
        logger.info("Streamlet {} is paused", id)
    }

    override fun resume() {
        inlet.resume()
        logger.info("Streamlet {} is resumed", id)
    }

    private fun commit() {
        logger.info("Committing changes")
        outlet.commit(positions)
        inlet.commit(positions)
    }

    private fun captureChanges(batchSize: Int) {
        var record = inlet.read()
        var count = 0
        while (record != null) {
            if (!ensureSchema(record)) {
                if (count > 0) commit()
                count = 0 // reset counter
                continue
            }
            outlet.write(record)
            positions[record.kafkaPartition()] = record.kafkaOffset() + 1
            count++
            if (count >= batchSize) break
            record = inlet.read()
        }
        if (count > 0) commit()
    }

    private fun ensurePosition() {
        val committedPositions = outlet.committedPositions() ?: return
        positions.putAll(committedPositions)

        val committedOffsets = inlet.committedOffsets()
        val isUpToDate = positions.all { (partition, offset) ->
            committedOffsets[partition] == offset
        }
        if (isUpToDate) return

        logger.warn(
            """
            Table position is different to Kafka committed offsets:
                Iceberg table positions: {}
                Kafka committed offsets: {}
            """.trimIndent(),
            positions, committedOffsets
        )
        logger.warn("Seeking to position stored in Iceberg table")
        inlet.seekOffsets(positions)
    }

    private fun ensureSchema(record: SinkRecord): Boolean {
        val keySchema = record.keySchema()
        val valueSchema = record.valueSchema()
        val hashed = Objects.hash(keySchema, valueSchema)

        if (hashedSchema == hashed)
            return true

        if (hashedSchema != null) {
            outlet.migrate(keySchema, valueSchema)
        }
        outlet.updateSourceSchema(keySchema, valueSchema)
        hashedSchema = hashed
        return false
    }

    override fun close() {
        inlet.close()
        outlet.close()
    }

    private inline fun <R> withMDC(block: () -> R): R {
        try {
            MDC.put("streamlet.id", "${id.schema}.${id.table}")
            return block()
        } catch (e: Exception) {
            logger.error("Streamlet {} is failed", this)
            status = FAILED
            throw e
        } finally {
            MDC.clear()
        }
    }

    override fun toString(): String {
        return "DebeziumStreamlet(${id})"
    }
}
