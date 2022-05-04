package dev.alluvial.sink.iceberg.io

import dev.alluvial.backport.iceberg.io.PartitioningWriterFactory
import org.apache.iceberg.FileFormat
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties.*
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.io.TaskWriter
import org.apache.kafka.connect.sink.SinkRecord
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import org.apache.kafka.connect.data.Schema as KafkaSchema

class AlluvialTaskWriterFactory(private val table: Table) {
    companion object {
        private val PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)
        private val TASK_FORMATTER = DateTimeFormatter.ofPattern("HHmmss").withZone(ZoneOffset.UTC)
    }

    private var dataKafkaSchema: KafkaSchema? = null
    private var equalityDeleteKafkaSchema: KafkaSchema? = null
    private var positionDeleteKafkaSchema: KafkaSchema? = null
    private val fileFormat = table.properties()
        .getOrDefault(
            DEFAULT_FILE_FORMAT,
            DEFAULT_FILE_FORMAT_DEFAULT.uppercase()
        ).let(FileFormat::valueOf)
    private val targetFileSizeInBytes = table.properties()
        .getOrDefault(
            WRITE_TARGET_FILE_SIZE_BYTES,
            WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT.toString()
        ).toLong()

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
        val fileWriterFactory = AlluvialFileWriterFactory.buildFor(table) {
            dataFileFormat = fileFormat
            dataKafkaSchema = this@AlluvialTaskWriterFactory.dataKafkaSchema
            deleteFileFormat = fileFormat
            equalityDeleteKafkaSchema = this@AlluvialTaskWriterFactory.equalityDeleteKafkaSchema
            positionDeleteKafkaSchema = this@AlluvialTaskWriterFactory.positionDeleteKafkaSchema
        }

        val partitionId = PARTITION_FORMATTER.format(Instant.now()).toInt()
        val taskId = TASK_FORMATTER.format(Instant.now()).toLong()
        val outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(fileFormat)
            .build()

        val partitioningWriterFactory = PartitioningWriterFactory.builder(fileWriterFactory)
            .fileFactory(outputFileFactory)
            .io(table.io())
            .fileFormat(fileFormat)
            .targetFileSizeInBytes(targetFileSizeInBytes)
            .buildForFanoutPartition()

        val partitioner = if (table.spec().isPartitioned)
            AlluvialTaskWriter.partitionerFor(table.spec(), table.schema()) else
            AlluvialTaskWriter.unpartition
        return AlluvialTaskWriter(
            partitioningWriterFactory,
            table.spec(),
            table.io(),
            partitioner,
            table.schema(),
            table.schema().identifierFieldIds()
        )
    }
}
