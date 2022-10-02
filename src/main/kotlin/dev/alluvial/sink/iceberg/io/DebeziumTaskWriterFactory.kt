package dev.alluvial.sink.iceberg.io

import dev.alluvial.dedupe.KeyCache
import dev.alluvial.sink.iceberg.type.KafkaSchema
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.iceberg.FileFormat
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties.*
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.io.TaskWriter
import org.apache.iceberg.util.PropertyUtil
import org.apache.kafka.connect.sink.SinkRecord
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class DebeziumTaskWriterFactory(
    private val table: Table,
    private val keyCache: KeyCache<SinkRecord, *, *>? = null,
    private val registry: MeterRegistry,
    private val tags: Tags
) {
    companion object {
        private val PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)
        private val TASK_FORMATTER = DateTimeFormatter.ofPattern("HHmmss").withZone(ZoneOffset.UTC)
    }

    private var dataKafkaSchema: KafkaSchema? = null
    private var equalityDeleteKafkaSchema: KafkaSchema? = null
    private var positionDeleteKafkaSchema: KafkaSchema? = null
    private val fileFormat = PropertyUtil.propertyAsString(
        table.properties(),
        DEFAULT_FILE_FORMAT,
        DEFAULT_FILE_FORMAT_DEFAULT.uppercase()
    ).let(FileFormat::valueOf)
    private val targetFileSizeInBytes = PropertyUtil.propertyAsLong(
        table.properties(),
        WRITE_TARGET_FILE_SIZE_BYTES,
        WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
    )

    fun setDataKafkaSchema(schema: KafkaSchema) {
        dataKafkaSchema = schema
    }

    fun setEqualityDeleteKafkaSchema(schema: KafkaSchema) {
        this.equalityDeleteKafkaSchema = schema
    }

    fun setPositionDeleteKafkaSchema(schema: KafkaSchema) {
        this.positionDeleteKafkaSchema = schema
    }

    fun create(): TaskWriter<SinkRecord> {
        val fileWriterFactory = KafkaFileWriterFactory.buildFor(table) {
            dataFileFormat = fileFormat
            dataKafkaSchema = this@DebeziumTaskWriterFactory.dataKafkaSchema
            deleteFileFormat = fileFormat
            equalityDeleteKafkaSchema = this@DebeziumTaskWriterFactory.equalityDeleteKafkaSchema
            positionDeleteKafkaSchema = this@DebeziumTaskWriterFactory.positionDeleteKafkaSchema
        }

        val partitionId = PARTITION_FORMATTER.format(Instant.now()).toInt()
        val taskId = TASK_FORMATTER.format(Instant.now()).toLong()
        val outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(fileFormat)
            .build()

        val partitioningWriterFactory = PartitioningWriterFactory.builder(fileWriterFactory)
            .fileFactory(outputFileFactory)
            .io(table.io())
            .targetFileSizeInBytes(targetFileSizeInBytes)
            .buildForFanoutPartition()

        val partitioner = if (table.spec().isPartitioned)
            DebeziumTaskWriter.partitionerFor(table.spec(), dataKafkaSchema!!, table.schema()) else
            DebeziumTaskWriter.unpartition
        return DebeziumTaskWriter(
            partitioningWriterFactory,
            table.spec(),
            table.io(),
            partitioner,
            dataKafkaSchema!!,
            table.schema(),
            table.schema().identifierFieldIds(),
            keyCache,
            registry,
            tags
        )
    }
}
