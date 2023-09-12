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
import dev.alluvial.utils.withSpan
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Clock
import java.time.Instant
import java.time.LocalDate

class DebeziumStreamlet(
    override val name: String,
    streamConfig: StreamConfig,
    private val inlet: KafkaTopicInlet,
    private val outlet: IcebergTableOutlet,
    private val tracker: RecordTracker,
    private val schemaHandler: SchemaHandler,
    private val tracer: Tracer,
    private val meter: Meter,
) : Streamlet {
    companion object {
        private val logger = LoggerFactory.getLogger(DebeziumStreamlet::class.java)
    }

    private var pendingRecord: SinkRecord? = null
    private val clock = Clock.systemUTC()
    private val idleTimeoutMs = streamConfig.idleTimeout.toMillis()
    private val commitBatchSize = streamConfig.commitBatchSize
    private val commitTimespanMs = streamConfig.commitTimespan.toMillis()
    private val rotateByDateInTz = streamConfig.rotateByDateInTz

    @Volatile
    override var status = CREATED

    private val attrs = Attributes.of(stringKey("alluvial.streamlet"), name)
    private val schemaMigrationCounter = meter.counterBuilder("alluvial.streamlet.schema.migration")
        .setDescription("Streamlet schema migration count")
        .build()

    override fun run() = withMDC {
        tracer.withSpan("DebeziumStreamlet.run", attrs) {
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
    }

    override fun pause() {
        inlet.pause()
        logger.info("Streamlet {} is paused", name)
    }

    override fun resume() {
        inlet.resume()
        logger.info("Streamlet {} is resumed", name)
    }

    private fun commit() = tracer.withSpan("DebeziumStreamlet.commit", attrs) {
        val offsets = tracker.consumedBrokerOffsets()
        val summary = tracker.buildSummary()

        logger.info("Committing changes {}", offsets)
        status = COMMITTING
        outlet.commit(summary)
        inlet.commit(offsets)
        status = RUNNING
    }

    override fun shouldRun(): Boolean = tracer.withSpan("DebeziumStreamlet.shouldRun", attrs) {
        val lag = inlet.currentLag()
        if (lag <= 0) return false
        if (lag > commitBatchSize) return true
        return tracker.lastSourceTimestamp() < clock.millis() - commitTimespanMs
    }

    fun canTerminate(): Boolean {
        if (status != SUSPENDED) return false

        val lag = inlet.currentLag()
        val committedTime = tracker.committedSourceTimestamp() ?: Long.MIN_VALUE
        return lag <= 0 && committedTime < clock.millis() - idleTimeoutMs
    }

    private fun captureChanges(batchSize: Int) = tracer.withSpan("DebeziumStreamlet.captureChanges", attrs) {
        var record = pendingRecord?.also { pendingRecord = null } ?: inlet.read()
        var consumingDate: LocalDate? = null
        var count = 0 // count consumed records ignore tombstone
        var setSchema = false
        try {
            while (record != null) {
                // Tombstone record is ignored
                if (record.value() == null) {
                    tracker.update(record)
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
                outlet.write(record)
                tracker.update(record)
                count++

                if (count >= batchSize) break
                record = inlet.read()
            }

            if (count > 0) commit()
        } catch (ex: TableTruncatedException) {
            if (count > 0) commit()
            val summary = tracker.buildSummary()
            outlet.truncate(summary)
        } catch (ex: SchemaChangedException) {
            if (count > 0) commit()
            schemaHandler.migrateSchema(record!!)
            schemaMigrationCounter.add(1, attrs)
        }
    }

    private fun ensureOffsets() = tracer.withSpan("DebeziumStreamlet.ensureOffsets", attrs) {
        val outletOffsets = tracker.committedBrokerOffsets()
        val inletOffsets = inlet.committedOffsets()

        val isUpToDate = outletOffsets.all { (partition, offset) ->
            inletOffsets[partition] == offset
        }
        if (isUpToDate) return

        logger.warn(
            """
            Table offsets is different to Kafka committed offsets:
                Iceberg table offsets  : {}
                Kafka committed offsets: {}
            """.trimIndent(),
            outletOffsets, inletOffsets
        )
        logger.warn("Seeking to offsets stored in Iceberg table")
        inlet.seekOffsets(outletOffsets)
    }

    override fun close() {
        logger.warn("Closing {}", this)
        inlet.close()
        outlet.close()
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
}
