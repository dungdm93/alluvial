package dev.alluvial.sink.iceberg

import dev.alluvial.api.Outlet
import dev.alluvial.sink.iceberg.io.DebeziumTaskWriterFactory
import dev.alluvial.sink.iceberg.type.IcebergTable
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.fieldSchema
import dev.alluvial.stream.debezium.RecordTracker
import dev.alluvial.utils.withSpan
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import org.apache.iceberg.AlluvialRowDelta
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.io.TaskWriter
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory

class IcebergTableOutlet(
    val name: String,
    val table: IcebergTable,
    val tracker: RecordTracker,
    private val tracer: Tracer,
    private val meter: Meter,
) : Outlet {
    companion object {
        private val logger = LoggerFactory.getLogger(IcebergTableOutlet::class.java)

        private val OPERATORS = mapOf(
            "c" to Attributes.of(stringKey("op"), "create"),
            "r" to Attributes.of(stringKey("op"), "read"),
            "u" to Attributes.of(stringKey("op"), "update"),
            "d" to Attributes.of(stringKey("op"), "delete"),
            "t" to Attributes.of(stringKey("op"), "truncate"),
        )
    }

    private val attrs = Attributes.of(stringKey("alluvial.outlet"), name)
    private val opCounter = meter.counterBuilder("alluvial.task.writer.records")
        .setDescription("Total events written")
        .build()

    private var writer: TaskWriter<SinkRecord>? = null
    private val writerFactory = DebeziumTaskWriterFactory(table, tracker)

    fun write(record: SinkRecord) {
        if (writer == null) {
            logger.info("Create new TaskWriter")
            writer = writerFactory.create()
        }
        try {
            increaseRecordCount(record)
            writer!!.write(record)
        } catch (e: RuntimeException) {
            throw RuntimeException("Error while writing record: $record", e)
        }
    }

    fun commit(summary: Map<String, String> = emptyMap()) = tracer.withSpan("IcebergTableOutlet.commit", attrs) {
        val result = writer!!.complete()
        val rowDelta = AlluvialRowDelta.of(table)
            .validateFromHead()

        result.dataFiles().forEach(rowDelta::addRows)
        result.deleteFiles().forEach(rowDelta::addDeletes)
        rowDelta.validateDataFilesExist(result.referencedDataFiles().asIterable())

        summary.forEach(rowDelta::set)
        rowDelta.commit()
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
    fun truncate(summary: Map<String, String> = emptyMap()) = tracer.withSpan("IcebergTableOutlet.truncate", attrs) {
        logger.warn("Truncate table {}", table.name())
        val overwrite = table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue())
        summary.forEach(overwrite::set)
        overwrite.commit()
    }

    private fun increaseRecordCount(record: SinkRecord) {
        val value = record.value() as KafkaStruct? ?: return // Tombstone events
        val op = value.getString("op")
        val attr = OPERATORS[op] ?: return
        opCounter.add(1, attr)
    }

    override fun close() {
        writer?.abort()
    }

    override fun toString(): String {
        return "IcebergTableOutlet(${table})"
    }
}
