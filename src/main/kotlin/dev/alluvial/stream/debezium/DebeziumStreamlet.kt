package dev.alluvial.stream.debezium

import dev.alluvial.api.SchemaHandler
import dev.alluvial.api.Streamlet
import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaTopicInlet
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.io.Closeable
import java.time.Clock
import java.util.concurrent.TimeUnit
import java.util.function.ToDoubleFunction
import kotlin.math.max
import kotlin.system.measureTimeMillis

@Suppress("MemberVisibilityCanBePrivate")
class DebeziumStreamlet(
    override val name: String,
    val inlet: KafkaTopicInlet,
    val outlet: IcebergTableOutlet,
    val schemaHandler: SchemaHandler,
    streamConfig: StreamConfig,
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
    private var metrics: StreamletMetrics? = null

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
        logger.info("Committing changes")
        val durationMs = measureTimeMillis {
            outlet.commit(offsets, lastRecordTimestamp)
            inlet.commit(offsets)
        }
        metrics?.mCommitDuration?.record(durationMs, TimeUnit.MILLISECONDS)
        metrics?.mCommitCount?.increment()
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
        inlet.close()
        outlet.close()
        metrics?.close()
    }

    private inline fun <R> withMDC(block: () -> R): R {
        try {
            MDC.put("streamlet.name", name)
            return block()
        } catch (e: Exception) {
            close()
            logger.error("Streamlet {} is failed", name)
            status = FAILED
            throw e
        } finally {
            MDC.clear()
        }
    }

    override fun toString(): String {
        return "DebeziumStreamlet(${name})"
    }

    fun enableMetrics(registry: MeterRegistry, tags: Iterable<Tag>) {
        metrics = StreamletMetrics(registry, tags)
    }

    @Suppress("SameParameterValue")
    inner class StreamletMetrics(
        private val registry: MeterRegistry,
        private val tags: Iterable<Tag>
    ) : Closeable {
        val mStatus = registerGauge(
            "streamlet.status",
            this@DebeziumStreamlet
        ) { it.status.ordinal.toDouble() }
        val mLastRecordTimestamp = registerGauge(
            "streamlet.record.last-timestamp",
            this@DebeziumStreamlet
        ) { it.lastRecordTimestamp.toDouble() }
        val mCommitCount = registerCounter("streamlet.commit.count")
        val mCommitDuration = registerTimer("streamlet.commit.duration")
        val metrics = listOf(mStatus, mLastRecordTimestamp, mCommitCount, mCommitDuration)

        private fun <T> registerGauge(name: String, obj: T, toDoubleFunction: ToDoubleFunction<T>): Gauge {
            registry.gauge(name, tags, obj, toDoubleFunction)
            return registry[name].gauge()
        }

        private fun registerCounter(name: String): Counter {
            return registry.counter(name, tags)
        }

        private fun registerTimer(name: String): Timer {
            return registry.timer(name, tags)
        }

        override fun close() {
            metrics.forEach { it.close() }
        }
    }
}
