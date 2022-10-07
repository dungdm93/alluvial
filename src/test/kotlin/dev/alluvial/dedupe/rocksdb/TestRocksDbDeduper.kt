package dev.alluvial.dedupe.rocksdb

import com.google.common.primitives.Longs
import dev.alluvial.dedupe.backend.rocksdb.*
import dev.alluvial.runtime.DeduplicationConfig
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.structSchema
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.*
import java.io.File

class TestRocksDbDeduper {
    companion object {
        private val tempDir = File("/tmp/alluvial/test/rocksdb")
        private lateinit var config: DeduplicationConfig
        private lateinit var client: RocksDbClient
        private lateinit var deduperProvider: RocksDbDeduperProvider<SinkRecord>
        private lateinit var deduper: RocksDbDeduper<SinkRecord>
//        private val serializer = BasicRecordSerializer(listOf("id"))
        private val serializer = AvroSerializer(emptyMap())
        private const val table = "this is table!!"

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            tempDir.mkdirs()
            getBackend()
        }

        private fun getBackend() {
            config = DeduplicationConfig(kind = "rocksdb", path = tempDir.path)
            client = RocksDbClient.getOrCreate(config)
            deduperProvider = RocksDbDeduperProvider(client)
            deduper = deduperProvider.create(TableIdentifier.of(table), serializer)
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            client.close()
            assert(tempDir.deleteRecursively()) { "folder should be deleted" }
        }
    }

    @AfterEach
    fun afterEach() {
        deduper.abort()
        deduper.clear()
        deduper.commit()
    }

    private fun createRecord(id: Int): SinkRecord {
        val schema = structSchema { field("id", Schema.INT32_SCHEMA) }
        val value = KafkaStruct(schema).put("id", id)
        return SinkRecord("topic", 1, schema, value, schema, value, 0)
    }

    @Test
    fun testHasKey() {
        val record = createRecord(1)
        val recordKeyBytes = serializer.serialize(record)

        Assertions.assertFalse(deduper.contains(record))

        deduper.add(record)
        deduper.commit()
        Assertions.assertTrue(client.hasKey(table, recordKeyBytes))
        Assertions.assertTrue(deduper.contains(record))
    }

    @Test
    fun testHasKeyIncludeProgress() {
        val record = createRecord(2)

        Assertions.assertFalse(deduper.contains(record))

        deduper.add(record)
        Assertions.assertTrue(deduper.contains(record))
    }

    @Test
    fun testCommitValueIsTimestamp() {
        val record = createRecord(1)
        val tsBefore = System.currentTimeMillis()

        deduper.add(record)
        deduper.commit()

        val tsAfter = System.currentTimeMillis()
        val ts = Longs.fromByteArray(client.get(table, serializer.serialize(record)))
        Assertions.assertTrue(tsBefore <= ts)
        Assertions.assertTrue(ts <= tsAfter)
    }

    @Test
    fun testAbort() {
        val record = createRecord(1)

        deduper.add(record)
        Assertions.assertTrue(deduper.contains(record))

        deduper.abort()
        Assertions.assertFalse(deduper.contains(record))
    }

    private fun verifyRecordsExist(vararg records: SinkRecord) {
        records.forEach {
            Assertions.assertTrue(client.hasKey(table, serializer.serialize(it)))
        }
    }

    @Test
    fun testAdd() {
        val recordOne = createRecord(1)
        val recordTwo = createRecord(2)
        val recordThree = createRecord(3)

        deduper.add(recordOne)
        deduper.add(recordTwo)
        deduper.add(recordThree)
        deduper.commit()

        verifyRecordsExist(recordOne, recordTwo, recordThree)
    }

    @Test
    fun testDelete() {
        val notToBeDeleted = createRecord(1)
        val recordTwo = createRecord(2)
        val toBeDeleted = createRecord(3)

        deduper.add(notToBeDeleted)
        deduper.add(recordTwo)
        deduper.add(toBeDeleted)
        deduper.remove(createRecord(4))
        deduper.remove(toBeDeleted)
        deduper.commit()

        verifyRecordsExist(recordTwo)
        Assertions.assertTrue(client.hasKey(table, serializer.serialize(notToBeDeleted)))
        Assertions.assertFalse(client.hasKey(table, serializer.serialize(toBeDeleted)))
    }

    @Test
    fun testAddNullifyDelete() {
        val record = createRecord(1)
        deduper.remove(record)
        deduper.add(record)
        deduper.commit()
        Assertions.assertTrue(client.hasKey(table, serializer.serialize(record)))
    }

    @Test
    fun testDeleteNullifyAdd() {
        val record = createRecord(1)
        deduper.add(record)
        deduper.commit()
        verifyRecordsExist(record)

        // Calling `delete` should nullify existing `add`
        deduper.add(record)
        deduper.remove(record)
        deduper.commit()
        Assertions.assertFalse(client.hasKey(table, serializer.serialize(record)))
    }
}
