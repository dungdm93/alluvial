package dev.alluvial.sink.iceberg

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import dev.alluvial.api.Outlet
import dev.alluvial.sink.iceberg.io.AlluvialTaskWriterFactory
import dev.alluvial.sink.iceberg.type.IcebergTable
import dev.alluvial.sink.iceberg.type.KafkaSchema
import io.micrometer.core.instrument.*
import org.apache.iceberg.io.TaskWriter
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Clock
import java.time.Duration
import java.util.concurrent.TimeUnit

class IcebergTableOutlet(
    val name: String,
    val table: IcebergTable,
    registry: MeterRegistry,
) : Outlet {
    companion object {
        private val logger = LoggerFactory.getLogger(IcebergTableOutlet::class.java)
        private val mapper = JsonMapper()
        private val typeRef = object : TypeReference<Map<Int, Long>>() {}
        private const val ALLUVIAL_POSITION_PROP = "alluvial.position"
        private const val ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP = "alluvial.last-record.timestamp"
    }

    private val metrics = Metrics(registry)
    private var writer: TaskWriter<SinkRecord>? = null
    private val writerFactory = AlluvialTaskWriterFactory(table, registry, Tags.of("outlet", name))

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

    fun commit(positions: Map<Int, Long>, lastRecordTimestamp: Long) {
        val result = metrics.recordCommitData(writer!!::complete)
        val rowDelta = table.newRowDelta()

        result.dataFiles().forEach(rowDelta::addRows)
        metrics.increaseDatafiles(result.dataFiles().size)
        result.deleteFiles().forEach(rowDelta::addDeletes)
        metrics.increaseDeleteFiles(result.deleteFiles().size)
        rowDelta.validateDeletedFiles()
            .validateDataFilesExist(result.referencedDataFiles().asIterable())

        rowDelta.set(ALLUVIAL_POSITION_PROP, mapper.writeValueAsString(positions))
        rowDelta.set(ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP, lastRecordTimestamp.toString())

        metrics.recordCommitMetadata(rowDelta::commit)
        writer = null
    }

    fun updateSourceSchema(keySchema: KafkaSchema, valueSchema: KafkaSchema) {
        assert(writer == null) { "Must be commit before change Kafka schema" }
        val rowSchema = valueSchema.field("after").schema()
        writerFactory.setDataKafkaSchema(rowSchema)
        writerFactory.setEqualityDeleteKafkaSchema(keySchema)
    }

    /**
     * @return offsets of last snapshot or null
     * if outlet doesn't have any snapshot.
     */
    fun committedOffsets(): Map<Int, Long> {
        val serialized = table.currentSnapshot()
            ?.summary()
            ?.get(ALLUVIAL_POSITION_PROP)
            ?: return emptyMap()
        return mapper.readValue(serialized, typeRef)
    }

    /**
     * @return timestamp in millisecond of last snapshot or null
     * if outlet doesn't have any snapshot.
     */
    fun committedTimestamp(): Long? {
        return table.currentSnapshot()
            ?.timestampMillis()
    }

    /**
     * @return timestamp in millisecond of last record committed or null
     * if outlet doesn't have any snapshot.
     */
    fun lastRecordTimestamp(): Long? {
        val ts = table.currentSnapshot()
            ?.summary()
            ?.get(ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP)
            ?: return null
        return ts.toLong()
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

        private val commitDataDuration = LongTaskTimer.builder("alluvial.outlet.commit.data.duration")
            .tags(tags)
            .description("Outlet data write duration")
            .register(registry)

        private val commitMetadataDuration = LongTaskTimer.builder("alluvial.outlet.commit.metadata.duration")
            .tags(tags)
            .description("Outlet metadata write duration")
            .register(registry)

        private val recordWriteLag = Timer.builder("alluvial.outlet.write.record.lag")
            .tags(tags)
            .description("Duration from the time record appeared in Kafka until it is written")
            .register(registry)

        private val deleteFilesCount = Counter.builder("alluvial.outlet.commit.files")
            .tags(tags.and("type", "delete"))
            .description("Number of delete files")
            .register(registry)

        private val dataFilesCount = Counter.builder("alluvial.outlet.commit.files")
            .tags(tags.and("type", "data"))
            .description("Number of data files")
            .register(registry)

        val registeredMetrics = listOf(
            commitDataDuration, commitMetadataDuration, recordWriteLag, deleteFilesCount, dataFilesCount
        )

        fun <T> recordCommitData(block: () -> T): T {
            return commitDataDuration.record(block)
        }

        fun <T> recordCommitMetadata(block: () -> T): T {
            return commitMetadataDuration.record(block)
        }

        fun recordWriteLag(recordTs: Long) {
            recordWriteLag.record(clock.millis() - recordTs, TimeUnit.MILLISECONDS)
        }

        fun increaseDatafiles(amount: Int) {
            dataFilesCount.increment(amount.toDouble())
        }

        fun increaseDeleteFiles(amount: Int) {
            deleteFilesCount.increment(amount.toDouble())
        }

        override fun close() {
            registeredMetrics.forEach {
                it.close()
                registry.remove(it)
            }
        }
    }
}
