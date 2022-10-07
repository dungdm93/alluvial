package dev.alluvial.dedupe

import java.io.Closeable

interface DedupeBackend<K, V> : Closeable {
    fun createTableIfNeeded(table: String)
    fun dropTableIfExists(table: String)
    fun listTables() : List<String>
    fun refresh()

    fun put(table: String, key: K, value: V)
    fun get(table: String, key: K): V
    fun hasKey(table: String, key: K): Boolean
    fun delete(table: String, key: K)
}
