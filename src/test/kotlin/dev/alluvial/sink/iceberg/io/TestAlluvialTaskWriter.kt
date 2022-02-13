package dev.alluvial.sink.iceberg.io

import dev.alluvial.backport.iceberg.io.PartitioningWriterFactory
import org.apache.iceberg.FileFormat
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.TableProperties
import org.apache.iceberg.TableTestBase
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.data.Record
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.io.TaskWriter
import org.apache.iceberg.io.WriteResult
import org.apache.iceberg.util.StructLikeSet
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

@Suppress("SameParameterValue")
internal class TestAlluvialTaskWriter : TableTestBase(TABLE_VERSION) {
    companion object {
        private const val TABLE_VERSION = 2
    }

    private val format = FileFormat.AVRO
    private val offset = AtomicLong()
    private lateinit var kafkaSchema: KafkaSchema
    private lateinit var envelopeSchema: KafkaSchema

    @Before
    override fun setupTable() {
        this.tableDir = temp.newFolder()
        Assert.assertTrue(tableDir.delete()) // created by table create
        this.metadataDir = File(tableDir, "metadata")
    }

    private fun fieldId(name: String): Int {
        return table.schema().findField(name).fieldId()
    }

    private fun initTable(partitioned: Boolean) {
        val spec = if (partitioned) {
            PartitionSpec.builderFor(SCHEMA)
                .identity("data")
                .build()
        } else {
            PartitionSpec.unpartitioned()
        }
        table = create(SCHEMA, spec)
        table.updateProperties()
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, (8 * 1024).toString())
            .defaultFormat(format)
            .commit()

        kafkaSchema = SchemaBuilder.struct()
            .field("id", KafkaSchema.INT32_SCHEMA)
            .field("data", KafkaSchema.STRING_SCHEMA)
            .optional()
            .build()
        envelopeSchema = SchemaBuilder.struct()
            .field("op", KafkaSchema.STRING_SCHEMA)
            .field("before", kafkaSchema)
            .field("after", kafkaSchema)
    }

    private fun testCdcEvents(partitioned: Boolean) {
        var writer = createTaskWriter(fieldId("id"))

        // Start the 1st transaction.
        writer.write(createInsert(1, "aaa"))
        writer.write(createInsert(2, "bbb"))
        writer.write(createInsert(3, "ccc"))

        // Update <2, 'bbb'> to <2, 'ddd'>
        writer.write(createUpdate(2, "bbb", "ddd")) // 1 pos-delete and 1 eq-delete.

        // Update <1, 'aaa'> to <1, 'eee'>
        writer.write(createUpdate(1, "aaa", "eee")) // 1 pos-delete and 1 eq-delete.

        // Insert <4, 'fff'>
        writer.write(createInsert(4, "fff"))
        // Insert <5, 'ggg'>
        writer.write(createInsert(5, "ggg"))

        // Delete <3, 'ccc'>
        writer.write(createDelete(3, "ccc")) // 1 pos-delete and 1 eq-delete.

        var result = writer.complete()
        Assert.assertEquals(if (partitioned) 7 else 1, result.dataFiles().size)
        Assert.assertEquals(if (partitioned) 3 else 1, result.deleteFiles().size)
        commitTransaction(result)

        var expected = expectedRowSet(
            createRecord(1, "eee"),
            createRecord(2, "ddd"),
            createRecord(4, "fff"),
            createRecord(5, "ggg")
        )
        var actual = actualRowSet("*")
        Assert.assertEquals("Should have expected records.", expected, actual)

        // Start the 2nd transaction.
        writer = createTaskWriter(fieldId("id"))

        // Update <2, 'ddd'> to <2, 'hhh'>
        writer.write(createUpdate(2, "ddd", "hhh")) // 1 eq-delete

        // Update <5, 'ggg'> to <5, 'iii'>
        writer.write(createUpdate(5, "ggg", "iii")) // 1 eq-delete

        // Delete <4, 'fff'>
        writer.write(createDelete(4, "fff")) // 1 eq-delete.

        result = writer.complete()
        Assert.assertEquals(if (partitioned) 2 else 1, result.dataFiles().size)
        Assert.assertEquals(if (partitioned) 3 else 1, result.deleteFiles().size)
        commitTransaction(result)

        expected = expectedRowSet(
            createRecord(1, "eee"),
            createRecord(2, "hhh"),
            createRecord(5, "iii"),
        )
        actual = actualRowSet("*")
        Assert.assertEquals("Should have expected records", expected, actual)
    }


    @Test
    fun testUnpartitioned() {
        initTable(false)
        testCdcEvents(false)
    }

    @Test
    fun testPartitioned() {
        initTable(true)
        testCdcEvents(true)
    }

    private fun createInsert(id: Int, data: String): SinkRecord {
        val record = KafkaStruct(kafkaSchema)
            .put("id", id)
            .put("data", data)

        val envelope = KafkaStruct(envelopeSchema)
            .put("op", "c")
            .put("after", record)

        return SinkRecord(
            "test", 1,
            kafkaSchema, record,  // key
            envelopeSchema, envelope, // value
            offset.getAndIncrement()
        )
    }

    private fun createUpdate(id: Int, before: String, after: String): SinkRecord {
        val beforeRecord = KafkaStruct(kafkaSchema)
            .put("id", id)
            .put("data", before)
        val afterRecord = KafkaStruct(kafkaSchema)
            .put("id", id)
            .put("data", after)

        val envelope = KafkaStruct(envelopeSchema)
            .put("op", "u")
            .put("before", beforeRecord)
            .put("after", afterRecord)

        return SinkRecord(
            "test", 1,
            kafkaSchema, beforeRecord,  // key
            envelopeSchema, envelope, // value
            offset.getAndIncrement()
        )
    }

    private fun createDelete(id: Int, data: String): SinkRecord {
        val record = KafkaStruct(kafkaSchema)
            .put("id", id)
            .put("data", data)

        val envelope = KafkaStruct(envelopeSchema)
            .put("op", "d")
            .put("before", record)

        return SinkRecord(
            "test", 1,
            kafkaSchema, record,  // key
            envelopeSchema, envelope, // value
            offset.getAndIncrement()
        )
    }

    private fun createRecord(id: Int, data: String): Record {
        return GenericRecord.create(SCHEMA).apply {
            setField("id", id)
            setField("data", data)
        }
    }

    private fun createTaskWriter(vararg equalityFieldIds: Int): TaskWriter<SinkRecord> {
        val schema = table.schema()
        val equalityFieldNames = equalityFieldIds.map { schema.findField(it).name() }
        val fileWriterFactory = AlluvialFileWriterFactory.buildFor(table) {
            equalityDeleteRowSchema = schema.select(equalityFieldNames)
        }
        val outputFileFactory = OutputFileFactory.builderFor(table, 1, 1L)
            .build()
        val partitioningWriterFactory = PartitioningWriterFactory.builder(fileWriterFactory)
            .fileFactory(outputFileFactory)
            .io(table.io())
            .fileFormat(format)
            .targetFileSizeInBytes(128 * 1024 * 1024)
            .buildForFanoutPartition()

        val partitioner = if (table.spec().isPartitioned) {
            AlluvialTaskWriter.partitionerFor(table.spec(), table.schema())
        } else {
            AlluvialTaskWriter.unpartition
        }
        return AlluvialTaskWriter(
            partitioningWriterFactory,
            table.spec(),
            table.io(),
            partitioner,
            table.schema(),
            equalityFieldIds.toSet()
        )
    }

    private fun expectedRowSet(vararg records: Record): StructLikeSet {
        val set = StructLikeSet.create(table.schema().asStruct())
        set.addAll(records)
        return set
    }

    private fun actualRowSet(vararg columns: String): StructLikeSet {
        return actualRowSet(null, *columns)
    }

    private fun actualRowSet(snapshotId: Long?, vararg columns: String): StructLikeSet {
        table.refresh()
        val set = StructLikeSet.create(table.schema().asStruct())
        val reader = IcebergGenerics
            .read(table)
            .useSnapshot(snapshotId ?: table.currentSnapshot().snapshotId())
            .select(*columns)
            .build()
        reader.use {
            it.forEach(set::add)
        }
        return set
    }

    private fun commitTransaction(result: WriteResult) {
        val rowDelta = table.newRowDelta()

        result.dataFiles().forEach(rowDelta::addRows)
        result.deleteFiles().forEach(rowDelta::addDeletes)
        rowDelta.validateDeletedFiles()
            .validateDataFilesExist(result.referencedDataFiles().toList())
            .commit()
    }
}
