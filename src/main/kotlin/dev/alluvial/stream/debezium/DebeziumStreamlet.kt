package dev.alluvial.stream.debezium

import dev.alluvial.api.SchemaHandler
import dev.alluvial.api.Streamlet
import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaTopicInlet
import dev.alluvial.source.kafka.sourceTimestamp
import dev.alluvial.utils.SchemaChangedException
import dev.alluvial.utils.TableTruncatedException
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.iceberg.BROKER_OFFSETS_PROP
import org.apache.iceberg.SOURCE_TIMESTAMP_PROP
import org.apache.iceberg.mapper
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.io.Closeable
import java.time.Clock
import java.time.Instant
import java.time.LocalDate

@Suppress("MemberVisibilityCanBePrivate")
class DebeziumStreamlet(
    override val name: String,
    val inlet: KafkaTopicInlet,
    val outlet: IcebergTableOutlet,
    val schemaHandler: SchemaHandler,
    streamConfig: StreamConfig,
    registry: MeterRegistry,
) : Streamlet {
    companion object {
        private val logger = LoggerFactory.getLogger(DebeziumStreamlet::class.java)
    }

    private val offsets = mutableMapOf<Int, Long>()
    private var lastSourceTimestamp = Long.MIN_VALUE
    private var pendingRecord: SinkRecord? = null
    private val clock = Clock.systemUTC()
    private val idleTimeoutMs = streamConfig.idleTimeout.toMillis()
    private val commitBatchSize = streamConfig.commitBatchSize
    private val commitTimespanMs = streamConfig.commitTimespan.toMillis()
    private val rotateByDateInTz = streamConfig.rotateByDateInTz
    private val metrics = Metrics(registry)

    @Volatile
    override var status = CREATED

    override fun run() = withMDC {
        if (status != CREATED) resume()
        status = RUNNING
        logger.info("Streamlet {} is running", name)
        ensureOffsets()
        while (shouldRun()) {
            captureChanges(commitBatchSize)
        }
        pause()
        status = SUSPENDED
    }

    override fun pause() {
        inlet.pause()
        logger.info("Streamlet {} is paused", name)
    }

    override fun resume() {
        inlet.resume()
        logger.info("Streamlet {} is resumed", name)
    }

    private fun commit() {
        logger.info("Committing changes {}", offsets)
        metrics.recordCommit {
            status = COMMITTING
            val summary = buildSummary()
            outlet.commit(summary)
            inlet.commit(offsets)
            status = RUNNING
        }
    }

    override fun shouldRun(): Boolean {
        val lag = inlet.currentLag()
        if (lag <= 0) return false
        if (lag > commitBatchSize) return true
        return lastSourceTimestamp < clock.millis() - commitTimespanMs
    }

    fun canTerminate(): Boolean {
        if (status != SUSPENDED) return false

        val lag = inlet.currentLag()
        val committedTime = outlet.committedSourceTimestamp()
        return lag <= 0 && committedTime < clock.millis() - idleTimeoutMs
    }

    private fun captureChanges(batchSize: Int) {
        var record = pendingRecord?.also { pendingRecord = null } ?: inlet.read()
        var consumingDate: LocalDate? = null
        var count = 0 // count consumed records ignore tombstone
        var setSchema = false
        try {
            while (record != null) {
                // Tombstone record is ignored
                if (record.value() == null) {
                    trackRecordInfo(record)
                    if (count >= batchSize) break
                    record = inlet.read()
                    continue
                }

                // Checking snapshot should be cutoff by day
                val recordDate = Instant.ofEpochMilli(record.sourceTimestamp()!!)
                    .atOffset(rotateByDateInTz)
                    .toLocalDate()
                when {
                    consumingDate == null -> consumingDate = recordDate
                    consumingDate != recordDate -> {
                        pendingRecord = record
                        break
                    }
                }

                // Checking for schema updated
                if (schemaHandler.shouldMigrate(record)) {
                    pendingRecord = record
                    throw SchemaChangedException()
                }
                if (!setSchema) {
                    if (record.key() != null) outlet.updateKeySchema(record.keySchema())
                    outlet.updateValueSchema(record.valueSchema())
                    setSchema = true
                }

                // Tracking info & write the record
                trackRecordInfo(record)
                outlet.write(record)
                count++

                if (count >= batchSize) break
                record = inlet.read()
            }

            if (count > 0) commit()
        } catch (ex: TableTruncatedException) {
            if (count > 0) commit()
            val summary = buildSummary()
            outlet.truncate(summary)
        } catch (ex: SchemaChangedException) {
            if (count > 0) commit()
            schemaHandler.migrateSchema(record!!)
            metrics.incrementSchemaMigration()
        }
    }

    private fun ensureOffsets() {
        val outletOffsets = outlet.committedBrokerOffsets()
        offsets.putAll(outletOffsets)
        lastSourceTimestamp = outlet.committedSourceTimestamp()

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
        logger.warn("Closing {}", this)
        inlet.close()
        outlet.close()
        metrics.close()
    }

    private fun trackRecordInfo(record: SinkRecord) {
        offsets[record.kafkaPartition()] = record.kafkaOffset() + 1

        val sourceTimestamp = record.sourceTimestamp() ?: return
        if (lastSourceTimestamp <= sourceTimestamp)
            lastSourceTimestamp = sourceTimestamp
        else
            logger.warn(
                "Receive un-ordered message at partition {}, offset {}\n" +
                    "\tsourceTimestamp={}, lastSourceTimestamp={}",
                record.kafkaPartition(), record.kafkaOffset(),
                sourceTimestamp, lastSourceTimestamp
            )
    }

    private fun buildSummary(): Map<String, String> {
        return mapOf(
            SOURCE_TIMESTAMP_PROP to lastSourceTimestamp.toString(),
            BROKER_OFFSETS_PROP to mapper.writeValueAsString(offsets)
        )
    }

    private inline fun <R> withMDC(block: () -> R): R {
        try {
            MDC.put("name", name)
            return block()
        } catch (e: Exception) {
            logger.error("Streamlet {} is failed", name, e)
            status = FAILED
            try {
                close()
            } catch (ex: Exception) {
                e.addSuppressed(ex)
            }
            throw e
        } finally {
            MDC.clear()
        }
    }

    override fun toString(): String {
        return "DebeziumStreamlet(${name})"
    }

    private inner class Metrics(private val registry: MeterRegistry) : Closeable {
        private val tags: Tags = Tags.of("streamlet", this@DebeziumStreamlet.name)

        private val commitDuration = LongTaskTimer.builder("alluvial.streamlet.commit.duration")
            .tags(tags)
            .description("Streamlet commit duration")
            .register(registry)

        private val schemaMigration = Counter.builder("alluvial.streamlet.schema.migration")
            .tags(tags)
            .description("Streamlet schema migration count")
            .register(registry)

        private val meters = listOf(commitDuration, schemaMigration)

        fun <T> recordCommit(block: () -> T): T {
            return commitDuration.record(block)
        }

        fun incrementSchemaMigration() {
            schemaMigration.increment()
        }

        override fun close() {
            meters.forEach {
                registry.remove(it)
                it.close()
            }
        }
    }
}
