package dev.alluvial.sink.iceberg

import dev.alluvial.api.Outlet
import dev.alluvial.sink.iceberg.io.DebeziumTaskWriterFactory
import dev.alluvial.sink.iceberg.type.IcebergTable
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.source.kafka.fieldSchema
import dev.alluvial.stream.debezium.RecordTracker
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import org.apache.iceberg.AlluvialRowDelta
import org.apache.iceberg.ContentFile
import org.apache.iceberg.FileContent
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.io.TaskWriter
import org.apache.iceberg.io.WriteResult
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Clock
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

class IcebergTableOutlet(
    val name: String,
    val table: IcebergTable,
    val tracker: RecordTracker,
    registry: MeterRegistry,
) : Outlet {
    companion object {
        private val logger = LoggerFactory.getLogger(IcebergTableOutlet::class.java)
    }

    private val metrics = Metrics(registry)
    private var writer: TaskWriter<SinkRecord>? = null
    private val writerFactory = DebeziumTaskWriterFactory(table, tracker, registry, Tags.of("outlet", name))

    fun write(record: SinkRecord) {
        if (writer == null) {
            logger.info("Create new TaskWriter")
            writer = writerFactory.create()
        }
        try {
            writer!!.write(record)
            metrics.recordWriteLag(record.timestamp())
        } catch (e: RuntimeException) {
            throw RuntimeException("Error while writing record: $record", e)
        }
    }

    fun commit(summary: Map<String, String> = emptyMap()) {
        val result = metrics.recordCommitData(writer!!::complete)
        val rowDelta = AlluvialRowDelta.of(table)
            .validateFromHead()

        result.dataFiles().forEach(rowDelta::addRows)
        result.deleteFiles().forEach(rowDelta::addDeletes)
        rowDelta.validateDataFilesExist(result.referencedDataFiles().asIterable())
        metrics.measureWriteResult(result)

        summary.forEach(rowDelta::set)
        metrics.recordCommitMetadata(rowDelta::commit)
        writer = null
    }

    fun updateKeySchema(keySchema: KafkaSchema) {
        assert(writer == null) { "Must be commit before change Kafka schema" }
        writerFactory.setEqualityDeleteKafkaSchema(keySchema)
    }

    fun updateValueSchema(valueSchema: KafkaSchema) {
        assert(writer == null) { "Must be commit before change Kafka schema" }
        val rowSchema = valueSchema.fieldSchema("after")
        writerFactory.setDataKafkaSchema(rowSchema)
    }

    /**
     * Truncate table by creating new snapshot to overwrite all rows.
     */
    fun truncate(summary: Map<String, String> = emptyMap()) {
        logger.warn("Truncate table {}", table.name())
        val overwrite = table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue())
        summary.forEach(overwrite::set)
        overwrite.commit()
    }

    override fun close() {
        writer?.abort()
    }

    override fun toString(): String {
        return "IcebergTableOutlet(${table})"
    }

    private inner class Metrics(private val registry: MeterRegistry) : Closeable {
        private val tags = Tags.of("outlet", name)
        private val clock = Clock.systemUTC()

        private val commitDataDuration = LongTaskTimer.builder("alluvial.outlet.commit")
            .tags(tags).tag("step", "write_data")
            .description("Outlet commit data duration")
            .register(registry)

        private val commitMetadataDuration = LongTaskTimer.builder("alluvial.outlet.commit")
            .tags(tags).tag("step", "write_metadata")
            .description("Outlet commit metadata duration")
            .register(registry)

        private val recordWriteLag = Timer.builder("alluvial.outlet.write.record.lag")
            .tags(tags)
            .description("Duration from the time record appeared in Kafka until it is written")
            .register(registry)

        ///////////// Records per file /////////////
        private val recordCountSummaries = buildMap {
            FileContent.values().forEach {
                val name = it.name.lowercase()
                val summary = DistributionSummary.builder("alluvial.outlet.records")
                    .tags(tags).tag("content", name)
                    .description("Number of records per file")
                    .maximumExpectedValue(500_000.0)
                    .serviceLevelObjectives(1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 500_000.0)
                    .register(registry)
                put(it, summary)
            }
        }

        ///////////// File size in bytes /////////////
        private val fileSizeSummaries = buildMap {
            FileContent.values().forEach {
                val name = it.name.lowercase()
                val summary = DistributionSummary.builder("alluvial.outlet.file_size")
                    .tags(tags).tag("content", name)
                    .description("File size in bytes")
                    .baseUnit("bytes")
                    .maximumExpectedValue(512.0 * 1024 * 1024) // 512MiB
                    .serviceLevelObjectives(
                        1.0 * 1024, // 1KiB
                        10.0 * 1024, // 10KiB
                        100.0 * 1024, // 100KiB
                        1.0 * 1024 * 1024, // 1MiB
                        10.0 * 1024 * 1024, // 10MiB
                        32.0 * 1024 * 1024, // 32MiB
                        64.0 * 1024 * 1024, // 64MiB
                        128.0 * 1024 * 1024, // 128MiB
                        256.0 * 1024 * 1024, // 256MiB
                        512.0 * 1024 * 1024, // 512MiB
                    )
                    .register(registry)
                put(it, summary)
            }
        }

        private val meters = listOf(commitDataDuration, commitMetadataDuration, recordWriteLag) +
            recordCountSummaries.values +
            fileSizeSummaries.values

        fun <T> recordCommitData(block: Supplier<T>): T {
            return commitDataDuration.record(block)
        }

        fun <T> recordCommitMetadata(block: Supplier<T>): T {
            return commitMetadataDuration.record(block)
        }

        fun recordWriteLag(recordTs: Long) {
            recordWriteLag.record(clock.millis() - recordTs, TimeUnit.MILLISECONDS)
        }

        fun measureWriteResult(result: WriteResult) {
            result.dataFiles().forEach(::trackRecordCount)
            result.deleteFiles().forEach(::trackRecordCount)
            result.dataFiles().forEach(::trackFileSize)
            result.deleteFiles().forEach(::trackFileSize)
        }

        private fun trackRecordCount(file: ContentFile<*>) {
            val summary = recordCountSummaries[file.content()] ?: return
            summary.record(file.recordCount().toDouble())
        }

        private fun trackFileSize(file: ContentFile<*>) {
            val summary = fileSizeSummaries[file.content()] ?: return
            summary.record(file.fileSizeInBytes().toDouble())
        }

        override fun close() {
            meters.forEach {
                registry.remove(it)
                it.close()
            }
        }
    }
}
