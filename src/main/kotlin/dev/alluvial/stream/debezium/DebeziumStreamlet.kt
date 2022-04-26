package dev.alluvial.stream.debezium

import dev.alluvial.api.SchemaHandler
import dev.alluvial.api.Streamlet
import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.api.StreamletId
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaTopicInlet
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Clock
import kotlin.math.max
import kotlin.time.Duration.Companion.minutes

@Suppress("MemberVisibilityCanBePrivate")
class DebeziumStreamlet(
    override val id: StreamletId,
    val inlet: KafkaTopicInlet,
    val outlet: IcebergTableOutlet,
    val schemaHandler: SchemaHandler,
) : Streamlet {
    companion object {
        private val logger = LoggerFactory.getLogger(DebeziumStreamlet::class.java)
    }

    private val offsets = mutableMapOf<Int, Long>()
    private var lastRecordTimestamp = Long.MIN_VALUE
    private val clock = Clock.systemUTC()
    var commitBatchSize = 1000
    var commitTimespanMs = 10.minutes.inWholeMilliseconds

    @Volatile
    override var status = CREATED

    override fun run() = withMDC {
        if (status != CREATED) resume()
        status = RUNNING
        logger.info("Streamlet {} is running", id)
        ensureOffsets()
        while (shouldRun()) {
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
        outlet.commit(offsets, lastRecordTimestamp)
        inlet.commit(offsets)
    }

    override fun shouldRun(): Boolean {
        val lag = inlet.currentLag()
        if (lag <= 0) return false
        if (lag > commitBatchSize) return true
        return lastRecordTimestamp < clock.millis() - commitTimespanMs
    }

    private fun captureChanges(batchSize: Int) {
        var record = inlet.read()
        var count = 0
        var firstNonNull = false
        while (record != null) {
            if (schemaHandler.shouldMigrate(record)) {
                if (count > 0) commit()
                count = 0 // reset counter
                firstNonNull = false
                schemaHandler.migrateSchema(record)
                continue
            }
            if (!firstNonNull && record.value() != null) {
                outlet.updateSourceSchema(record.keySchema(), record.valueSchema())
                firstNonNull = true
            }
            outlet.write(record)
            offsets[record.kafkaPartition()] = record.kafkaOffset() + 1
            lastRecordTimestamp = max(lastRecordTimestamp, record.timestamp())
            count++
            if (count >= batchSize) break
            record = inlet.read()
        }
        if (count > 0) commit()
    }

    private fun ensureOffsets() {
        val outletOffsets = outlet.committedOffsets() ?: return
        offsets.putAll(outletOffsets)
        lastRecordTimestamp = outlet.lastRecordTimestamp() ?: Long.MIN_VALUE

        val inletOffsets = inlet.committedOffsets()
        val isUpToDate = offsets.all { (partition, offset) ->
            inletOffsets[partition] == offset
        }
        if (isUpToDate) return

        logger.warn(
            """
            Table offsets is different to Kafka committed offsets:
                Iceberg table offsets  : {}
                Kafka committed offsets: {}
            """.trimIndent(),
            offsets, inletOffsets
        )
        logger.warn("Seeking to offsets stored in Iceberg table")
        inlet.seekOffsets(offsets)
    }

    override fun close() {
        inlet.close()
        outlet.close()
    }

    private inline fun <R> withMDC(block: () -> R): R {
        try {
            MDC.put("streamlet.id", id.toString())
            return block()
        } catch (e: Exception) {
            logger.error("Streamlet {} is failed", id)
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
