package dev.alluvial.sink.iceberg.io

import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.structSchema
import dev.alluvial.stream.debezium.RecordTracker
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.iceberg.FileFormat
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.TableTestBase
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.data.Record
import org.apache.iceberg.io.TaskWriter
import org.apache.iceberg.io.WriteResult
import org.apache.iceberg.util.StructLikeSet
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Assert
import java.io.File
import java.util.concurrent.atomic.AtomicLong

@Suppress("SameParameterValue", "MemberVisibilityCanBePrivate")
internal open class TestDebeziumTaskWriterBase : TableTestBase(2) {
    private val offset = AtomicLong(0)
    protected lateinit var tracker: RecordTracker

    protected val sourceSchema = structSchema {
        field("ts_ms", KafkaSchema.INT64_SCHEMA)
        field("lsn", KafkaSchema.INT64_SCHEMA)
        optional() // "source" is required in debezium, but we make it optional for testing purpose
    }
    protected val rowSchema = structSchema {
        field("id", KafkaSchema.INT32_SCHEMA)
        field("data", KafkaSchema.STRING_SCHEMA)
        optional()
    }
    protected val valueSchema = structSchema {
        field("op", KafkaSchema.STRING_SCHEMA)
        field("before", rowSchema)
        field("after", rowSchema)
        field("source", sourceSchema)
    }
    protected val keySchema = structSchema {
        field("id", KafkaSchema.STRING_SCHEMA)
    }

    protected fun createTable(spec: PartitionSpec) {
        tableDir = temp.newFolder()
        Assert.assertTrue(tableDir.delete()) // created by table create
        metadataDir = File(tableDir, "metadata")

        table = create(SCHEMA, spec)
        table.updateSchema()
            .setIdentifierFields("id")
            .commit()
        table.updateProperties()
            .defaultFormat(FileFormat.AVRO)
            .commit()
    }

    protected fun createTaskWriter(sSchema: KafkaSchema): TaskWriter<SinkRecord> {
        val factory = DebeziumTaskWriterFactory(table, tracker, SimpleMeterRegistry(), Tags.empty())
        factory.setDataKafkaSchema(sSchema)
        return factory.create()
    }

    protected fun writeRecords(vararg records: SinkRecord): WriteResult {
        val writer = createTaskWriter(rowSchema)
        for (r in records) {
            writer.write(r)
            tracker.update(r)
        }
        val result = writer.complete()
        commitTransaction(result)
        return result
    }

    protected fun rowFor(id: Int, data: String): KafkaStruct {
        return KafkaStruct(rowSchema)
            .put("id", id)
            .put("data", data)
    }

    protected fun sourceFor(sourceTs: Long, lsn: Long): KafkaStruct {
        return KafkaStruct(sourceSchema)
            .put("ts_ms", sourceTs)
            .put("lsn", lsn)
    }

    protected fun recordFor(
        op: String,
        key: KafkaStruct?,
        before: KafkaStruct?,
        after: KafkaStruct?,
        source: KafkaStruct?,
    ): SinkRecord {
        val value = KafkaStruct(valueSchema)
            .put("op", op)
            .put("before", before)
            .put("after", after)
            .put("source", source)

        return SinkRecord(
            "test", 1,
            rowSchema, key,  // key
            valueSchema, value, // value
            offset.getAndIncrement()
        )
    }

    protected fun iRecord(id: Int, data: String): Record {
        return GenericRecord.create(SCHEMA).apply {
            setField("id", id)
            setField("data", data)
        }
    }

    protected fun commitTransaction(result: WriteResult) {
        val rowDelta = table.newRowDelta()

        result.dataFiles().forEach(rowDelta::addRows)
        result.deleteFiles().forEach(rowDelta::addDeletes)
        rowDelta.validateDeletedFiles()
            .validateDataFilesExist(result.referencedDataFiles().toList())
            .commit()
    }

    protected fun Array<Record>.toRowSet(): StructLikeSet {
        val set = StructLikeSet.create(table.schema().asStruct())
        set.addAll(this)
        return set
    }

    protected fun Iterable<Record>.toRowSet(): StructLikeSet {
        val set = StructLikeSet.create(table.schema().asStruct())
        set.addAll(this)
        return set
    }

    protected fun Table.read(vararg columns: String): List<Record> {
        return this.read(null, *columns)
    }

    protected fun Table.read(snapshotId: Long?, vararg columns: String): List<Record> {
        this.refresh()
        val reader = IcebergGenerics
            .read(this)
            .useSnapshot(snapshotId ?: this.currentSnapshot().snapshotId())
            .select(*columns)
            .build()
        reader.use {
            return it.toList()
        }
    }

    protected fun verify(expected: Array<Record>) {
        val actual = table.read("*")
        Assert.assertEquals(expected.size, actual.size)
        Assert.assertEquals(
            expected.mapTo(mutableSetOf()) { it.getField("id") },
            actual.mapTo(mutableSetOf()) { it.getField("id") },
        )
        Assert.assertEquals(expected.toRowSet(), actual.toRowSet())
    }
}
