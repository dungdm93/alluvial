package dev.alluvial.dedupe.rocksdb

import com.google.common.primitives.Longs
import dev.alluvial.dedupe.backend.rocksdb.RocksDbBackend
import dev.alluvial.dedupe.backend.rocksdb.RocksDbKeyCache
import dev.alluvial.runtime.DeduplicationConfig
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.structSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.TempDir
import java.io.File

class TestRocksDBKeyCache {
    companion object {
        @TempDir
        private lateinit var tempDir: File
        private lateinit var config: DeduplicationConfig
        private lateinit var backend: RocksDbBackend
        private const val table = "this is table!!"

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            getBackend()
        }

        private fun getBackend() {
            config = DeduplicationConfig(kind = "rocksdb", path = tempDir.path)
            backend = RocksDbBackend.getOrCreate(config)
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            backend.close()
            assert(tempDir.deleteRecursively()) { "folder should be deleted" }
        }
    }

    @AfterEach
    fun afterEach() {
        backend.dropTableIfExists(table)
    }

    private fun createRecord(id: Int): SinkRecord {
        val schema = structSchema { field("id", Schema.INT32_SCHEMA) }
        val value = KafkaStruct(schema).put("id", id)
        return SinkRecord("topic", 1, schema, value, schema, value, 0)
    }

    @Test
    fun testHasKey() {
        val cache = RocksDbKeyCache(table, backend, listOf("id"))
        val record = createRecord(1)
        val recordKeyBytes = cache.serializeKey(record)

        Assertions.assertFalse(cache.hasKey(record))

        backend.put(table, recordKeyBytes, "magic value".toByteArray())
        Assertions.assertTrue(backend.hasKey(table, recordKeyBytes))
        Assertions.assertTrue(cache.hasKey(record))
    }

    @Test
    fun testHasKeyIncludeProgress() {
        val cache = RocksDbKeyCache(table, backend, listOf("id"))
        val record = createRecord(2)

        Assertions.assertFalse(cache.hasKey(record))

        cache.add(record)
        cache.commit()
        Assertions.assertTrue(cache.hasKey(record))
    }

    @Test
    fun testSerializeKey() {
        val schema = structSchema {
            field("id", Schema.INT32_SCHEMA)
            field("type", Schema.STRING_SCHEMA)
        }
        val value = KafkaStruct(schema).put("id", 1).put("type", "basic")
        val record = SinkRecord("topic", 1, schema, value, schema, value, 0)
        val ids = record.keySchema().fields().map { it.name() }
        val cache = RocksDbKeyCache(table, backend, ids)
        val recordKeyBytes = cache.serializeKey(record)
        Assertions.assertEquals("1-basic", recordKeyBytes.decodeToString())
    }

    @Test
    fun testCommitValueIsTimestamp() {
        val cache = RocksDbKeyCache(table, backend, listOf("id"))
        val record = createRecord(1)
        val tsBefore = System.currentTimeMillis()

        cache.add(record)
        cache.commit()

        val tsAfter = System.currentTimeMillis()
        val ts = Longs.fromByteArray(backend.get(table, cache.serializeKey(record)))
        Assertions.assertTrue(tsBefore <= ts)
        Assertions.assertTrue(ts <= tsAfter)
    }

    @Test
    fun testAbort() {
        val cache = RocksDbKeyCache(table, backend, listOf("id"))
        val record = createRecord(1)

        cache.add(record)
        Assertions.assertTrue(cache.hasKey(record))

        cache.abort()
        Assertions.assertFalse(cache.hasKey(record))
    }
}
