package dev.alluvial.dedupe

import dev.alluvial.api.KVBackend
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestKeyCache {
    companion object {
        const val table = "Oh a table!"
    }
    @Test
    fun testAdd() {
        val backend = MockKVBackend()
        val cache = MockKeyCache(table, backend)

        cache.add("hello")
        cache.add("my")
        cache.add("ehcac")
        cache.commit()

        Assertions.assertEquals(3, backend.data[table]!!.size)
    }

    @Test
    fun testDelete() {
        val backend = MockKVBackend()
        val cache = MockKeyCache(table, backend)

        cache.add("hello")
        cache.add("my")
        cache.add("cache")
        cache.delete("ehcac")
        cache.delete("cache")
        cache.commit()

        val data = backend.data[table]!!
        Assertions.assertNotNull(data)
        Assertions.assertEquals(2, data.size)
        Assertions.assertTrue(backend.hasKey(table, "hello"))
        Assertions.assertTrue(backend.hasKey(table, "hello"))
    }

    @Test
    fun testAddNullifyDelete() {
        val table = "A table!"
        val backend = MockKVBackend()
        val cache = MockKeyCache(table, backend)

        cache.delete("hello")
        cache.add("hello")
        cache.commit()
        Assertions.assertTrue(backend.hasKey(table, "hello"))
    }

    @Test
    fun testDeleteNullifyAdd() {
        val backend = MockKVBackend()
        val cache = MockKeyCache(table, backend)

        cache.add("hello")
        cache.commit()
        val data = backend.data[table]!!
        Assertions.assertEquals(1, data.size)

        // Calling `delete` should nullify existing `add`
        cache.add("hello")
        cache.delete("hello")
        cache.commit()
        Assertions.assertEquals(0, data.size)
    }

    class MockKVBackend : KVBackend<String, String> {
        val data = mutableMapOf<String, MutableMap<String, String>>()

        override fun createTableIfNeeded(table: String) {
            data.computeIfAbsent(table) { mutableMapOf() }
        }

        override fun dropTableIfExists(table: String) {
            data.remove(table)
        }

        override fun listTables(): List<String> = data.keys.toList()

        override fun refresh() {}

        override fun put(table: String, key: String, value: String) {
            data[table]!![key] = value
        }

        override fun get(table: String, key: String): String = data[table]!![key]!!
        override fun hasKey(table: String, key: String): Boolean = data[table]!!.containsKey(key)
        override fun delete(table: String, key: String) {
            data[table]!!.remove(key)
        }

        override fun close() {}
    }

    class MockKeyCache(private val table: String, private val backend: KVBackend<String, String>) :
        KeyCache<String, String, String>(table, backend) {

        override fun hasKey(record: String) = backend.hasKey(table, record)

        override fun commit() {
            val magicValue = "oh magic!"
            addedEntries.forEach{ backend.put(table, it, magicValue) }
            deletedEntries.forEach{ backend.delete(table, it) }
        }

        override fun serializeKey(record: String) = "this-is-a-key:$record"
    }
}
