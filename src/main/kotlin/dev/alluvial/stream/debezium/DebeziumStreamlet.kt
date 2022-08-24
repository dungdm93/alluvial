package dev.alluvial.stream.debezium

import dev.alluvial.api.SchemaHandler
import dev.alluvial.api.Streamlet
import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.sink.iceberg.ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP
import dev.alluvial.sink.iceberg.ALLUVIAL_POSITION_PROP
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.sink.iceberg.mapper
import dev.alluvial.source.kafka.KafkaTopicInlet
import dev.alluvial.utils.TableTruncatedException
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.io.Closeable
import java.time.Clock
import kotlin.math.max

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
    private var lastRecordTimestamp = Long.MIN_VALUE
    private val clock = Clock.systemUTC()
    private val idleTimeoutMs = streamConfig.idleTimeout.toMillis()
    private val commitBatchSize = streamConfig.commitBatchSize
    private val commitTimespanMs = streamConfig.commitTimespan.toMillis()
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
        return lastRecordTimestamp < clock.millis() - commitTimespanMs
    }

    fun canTerminate(): Boolean {
        if (status != SUSPENDED) return false

        val lag = inlet.currentLag()
        val committedTime = outlet.committedTimestamp() ?: Long.MIN_VALUE
        return lag <= 0 && committedTime < clock.millis() - idleTimeoutMs
    }

    private fun captureChanges(batchSize: Int) {
        var record = inlet.read()
        var count = 0
        var keySchemaIsSet = false
        var valueSchemaIsSet = false
        try {
            while (record != null) {
                if (schemaHandler.shouldMigrate(record)) {
                    if (count > 0) commit()
                    count = 0 // reset counter
                    schemaHandler.migrateSchema(record)
                    keySchemaIsSet = false
                    valueSchemaIsSet = false
                    metrics.incrementSchemaMigration()
                }

                offsets[record.kafkaPartition()] = record.kafkaOffset() + 1
                lastRecordTimestamp = max(lastRecordTimestamp, record.timestamp())

                if (!keySchemaIsSet && record.key() != null) {
                    outlet.updateKeySchema(record.keySchema())
                    keySchemaIsSet = true
                }

                // Tombstone event is ignored
                if (record.value() != null) {
                    if (!valueSchemaIsSet) {
                        outlet.updateValueSchema(record.valueSchema())
                        valueSchemaIsSet = true
                    }
                    outlet.write(record)
                    count++
                }

                if (count >= batchSize) break
                record = inlet.read()
            }

            if (count > 0) commit()
        } catch (ex: TableTruncatedException) {
            if (count > 0) commit()
            val summary = buildSummary()
            outlet.truncate(summary)
        }
    }

    private fun ensureOffsets() {
        val outletOffsets = outlet.committedOffsets()
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
        logger.warn("Closing {}", this)
        inlet.close()
        outlet.close()
        metrics.close()
    }

    private fun buildSummary(): Map<String, String> {
        return mapOf(
            ALLUVIAL_POSITION_PROP to mapper.writeValueAsString(offsets),
            ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP to lastRecordTimestamp.toString()
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

        private val lastRecordTimestamp = Gauge.builder(
            "alluvial.streamlet.record.last-timestamp", this@DebeziumStreamlet
        ) { it.lastRecordTimestamp.toDouble() }
            .tags(tags)
            .description("Streamlet last record timestamp")
            .register(registry)

        private val commitDuration = LongTaskTimer.builder("alluvial.streamlet.commit.duration")
            .tags(tags)
            .description("Streamlet commit duration")
            .register(registry)

        private val schemaMigration = Counter.builder("alluvial.streamlet.schema.migration")
            .tags(tags)
            .description("Streamlet schema migration count")
            .register(registry)

        private val registeredMetrics = listOf(lastRecordTimestamp, commitDuration, schemaMigration)

        fun <T> recordCommit(block: () -> T): T {
            return commitDuration.record(block)
        }

        fun incrementSchemaMigration() {
            schemaMigration.increment()
        }

        override fun close() {
            registeredMetrics.forEach {
                it.close()
                registry.remove(it)
            }
        }
    }
}
