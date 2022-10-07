package dev.alluvial.sink.iceberg.io

import dev.alluvial.dedupe.backend.rocksdb.*
import dev.alluvial.runtime.DeduplicationConfig
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.structSchema
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.iceberg.FileFormat
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.TableProperties
import org.apache.iceberg.TableTestBase
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.data.Record
import org.apache.iceberg.io.*
import org.apache.iceberg.util.StructLikeSet
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Assert
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.AfterAll
import java.io.File
import java.util.concurrent.atomic.AtomicLong

@Suppress("SameParameterValue")
internal class TestDebeziumTaskWriter : TableTestBase(TABLE_VERSION) {
    companion object {
        private const val TABLE_VERSION = 2
    }

    private val format = FileFormat.AVRO
    private val offset = AtomicLong()
    private lateinit var keySchema: KafkaSchema
    private lateinit var kafkaSchema: KafkaSchema
    private lateinit var envelopeSchema: KafkaSchema
    private lateinit var rockDBPath: File

    @Before
    override fun setupTable() {
        this.tableDir = temp.newFolder()
        Assert.assertTrue(tableDir.delete()) // created by table create
        this.metadataDir = File(tableDir, "metadata")
    }

    @Before
    fun setup() {
        rockDBPath = File("/tmp/alluvial/test/rocksdb")
        rockDBPath.mkdirs()
    }

    @AfterAll
    fun afterAll() {
        rockDBPath.deleteRecursively()
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

        keySchema = structSchema {
            field("id", KafkaSchema.INT32_SCHEMA)
        }
        kafkaSchema = structSchema {
            field("id", KafkaSchema.INT32_SCHEMA)
            field("data", KafkaSchema.STRING_SCHEMA)
            optional()
        }
        envelopeSchema = structSchema {
            field("op", KafkaSchema.STRING_SCHEMA)
            field("before", kafkaSchema)
            field("after", kafkaSchema)
        }
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
    @Ignore
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
        val key = KafkaStruct(keySchema)
            .put("id", id)
        val record = KafkaStruct(kafkaSchema)
            .put("id", id)
            .put("data", data)

        val envelope = KafkaStruct(envelopeSchema)
            .put("op", "c")
            .put("after", record)

        return SinkRecord(
            "test", 1,
            keySchema, key,  // key
            envelopeSchema, envelope, // value
            offset.getAndIncrement()
        )
    }

    private fun createUpdate(id: Int, before: String, after: String): SinkRecord {
        val key = KafkaStruct(keySchema)
            .put("id", id)
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
            keySchema, key,  // key
            envelopeSchema, envelope, // value
            offset.getAndIncrement()
        )
    }

    private fun createRead(id: Int, after: String): SinkRecord {
        val key = KafkaStruct(keySchema)
            .put("id", id)
        val afterRecord = KafkaStruct(kafkaSchema)
            .put("id", id)
            .put("data", after)

        val envelope = KafkaStruct(envelopeSchema)
            .put("op", "r")
            .put("after", afterRecord)

        return SinkRecord(
            "test", 1,
            keySchema, key,  // key
            envelopeSchema, envelope, // value
            offset.getAndIncrement()
        )
    }

    private fun createDelete(id: Int, data: String): SinkRecord {
        val key = KafkaStruct(keySchema)
            .put("id", id)
        val record = KafkaStruct(kafkaSchema)
            .put("id", id)
            .put("data", data)

        val envelope = KafkaStruct(envelopeSchema)
            .put("op", "d")
            .put("before", record)

        return SinkRecord(
            "test", 1,
            keySchema, key,  // key
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

    private fun createTaskWriter(vararg equalityFieldIds: Int, deduper: RocksDbDeduper<SinkRecord>? = null): TaskWriter<SinkRecord> {
        val schema = table.schema()
        val equalityFieldNames = equalityFieldIds.map { schema.findField(it).name() }
        val fileWriterFactory = KafkaFileWriterFactory.buildFor(table) {
            equalityDeleteRowSchema = schema.select(equalityFieldNames)
        }
        val outputFileFactory = OutputFileFactory.builderFor(table, 1, 1L)
            .build()
        val partitioningWriterFactory = PartitioningWriterFactory.builder(fileWriterFactory)
            .fileFactory(outputFileFactory)
            .io(table.io())
            .targetFileSizeInBytes(128 * 1024 * 1024)
            .buildForFanoutPartition()

        val partitioner = if (table.spec().isPartitioned) {
            DebeziumTaskWriter.partitionerFor(table.spec(), kafkaSchema, table.schema())
        } else {
            DebeziumTaskWriter.unpartition
        }
        return DebeziumTaskWriter(
            partitioningWriterFactory,
            table.spec(),
            table.io(),
            partitioner,
            kafkaSchema,
            table.schema(),
            equalityFieldIds.toSet(),
            deduper,
            SimpleMeterRegistry(),
            Tags.of("outlet", table.name())
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

    private fun getRecordReader(snapshotId: Long?, vararg columns: String): CloseableIterable<Record> {
        table.refresh()
        return IcebergGenerics
            .read(table)
            .useSnapshot(snapshotId ?: table.currentSnapshot().snapshotId())
            .select(*columns)
            .build()
    }

    private fun actualRowSet(snapshotId: Long?, vararg columns: String): StructLikeSet {
        val set = StructLikeSet.create(table.schema().asStruct())
        val reader = getRecordReader(snapshotId, *columns)
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

    private fun getTableCount(): Int {
        getRecordReader(null, "*").use {
            return it.count()
        }
    }

    private fun writeAndCommitCache(deduper: RocksDbDeduper<SinkRecord>, vararg records: SinkRecord) {
        val writer = createTaskWriter(fieldId("id"), deduper = deduper)
        records.forEach(writer::write)
        commitTransaction(writer.complete())
        deduper.commit()
    }

    @Test
    fun testDeduplicateCreateEvents() {
        initTable(false)
        val tableName = table.name()
        val config = DeduplicationConfig(kind = "rocksdb", path = rockDBPath.path)
        val client = RocksDbClient.getOrCreate(config)
        val deduperProvider = RocksDbDeduperProvider<SinkRecord>(client)
//        val serializer = BasicRecordSerializer(listOf("id"))
        val serializer = AvroSerializer(emptyMap())
        val deduper = deduperProvider.create(TableIdentifier.of(tableName), serializer)

        // "read" event => upsert record & put to cache
        val readFirst = createRead(1, "first")
        val firstKeyBytes = serializer.serialize(readFirst)
        writeAndCommitCache(deduper, readFirst)
        Assert.assertTrue(client.hasKey(tableName, firstKeyBytes))

        // Same "read" event => upsert record & put to cache (again)
        writeAndCommitCache(deduper, readFirst)
        Assert.assertEquals(1, getTableCount())

        // "create" event of the same record => ignore
        val createFirst = createInsert(1, "firstNew")
        writeAndCommitCache(deduper, createFirst)
        Assert.assertEquals(1, getTableCount())

        // "delete" event => delete record & remove from cache
        val deleteFirst = createDelete(1, "first")
        writeAndCommitCache(deduper, deleteFirst)
        Assert.assertFalse(client.hasKey(tableName, firstKeyBytes))
        Assert.assertEquals(0, getTableCount())

        // "create" event => re-create record & put to cache
        writeAndCommitCache(deduper, createFirst)
        Assert.assertTrue(client.hasKey(tableName, firstKeyBytes))
        Assert.assertEquals(1, getTableCount())

        // "create" event of another record => create & put to cache
        val createSecond = createInsert(2, "second")
        val secondKeyBytes = serializer.serialize(createSecond)
        writeAndCommitCache(deduper, createSecond)
        Assert.assertTrue(client.hasKey(tableName, secondKeyBytes))
        Assert.assertEquals(2, getTableCount())

        val expected = expectedRowSet(
            createRecord(1, "firstNew"),
            createRecord(2, "second"),
        )
        val actual = actualRowSet("*")
        Assert.assertEquals("Should have expected records", expected, actual)

        client.close()
    }
}
