package dev.alluvial.sink.iceberg

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import dev.alluvial.api.Outlet
import dev.alluvial.api.StreamletId
import dev.alluvial.sink.iceberg.io.AlluvialTaskWriterFactory
import dev.alluvial.sink.iceberg.type.IcebergTable
import dev.alluvial.sink.iceberg.type.KafkaSchema
import org.apache.iceberg.io.TaskWriter
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory

class IcebergTableOutlet(
    val id: StreamletId,
    val table: IcebergTable,
) : Outlet {
    companion object {
        private val logger = LoggerFactory.getLogger(IcebergTableOutlet::class.java)
        private val mapper = JsonMapper()
        private val typeRef = object : TypeReference<Map<Int, Long>>() {}
        private const val ALLUVIAL_POSITION_PROP = "alluvial.position"
        private const val ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP = "alluvial.last-record.timestamp"
    }

    private val writerFactory = AlluvialTaskWriterFactory(table)
    private var writer: TaskWriter<SinkRecord>? = null

    fun write(record: SinkRecord) {
        if (writer == null) {
            logger.info("Create new TaskWriter")
            writer = writerFactory.create()
        }
        writer!!.write(record)
    }

    fun commit(positions: Map<Int, Long>, lastRecordTimestamp: Long) {
        val result = writer!!.complete()
        val rowDelta = table.newRowDelta()

        result.dataFiles().forEach(rowDelta::addRows)
        result.deleteFiles().forEach(rowDelta::addDeletes)
        rowDelta.validateDeletedFiles()
            .validateDataFilesExist(result.referencedDataFiles().asIterable())

        rowDelta.set(ALLUVIAL_POSITION_PROP, mapper.writeValueAsString(positions))
        rowDelta.set(ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP, lastRecordTimestamp.toString())

        rowDelta.commit()
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
    fun committedOffsets(): Map<Int, Long>? {
        val serialized = table.currentSnapshot()
            ?.summary()
            ?.get(ALLUVIAL_POSITION_PROP)
            ?: return null
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
}
