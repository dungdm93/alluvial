package dev.alluvial.dedupe

import dev.alluvial.api.KVBackend

abstract class KeyCache<S, K, V>(table: String, backend: KVBackend<K, V>) {
    protected val addedEntries = mutableSetOf<S>()
    protected val deletedEntries = mutableSetOf<S>()

    init {
        backend.createTableIfNeeded(table)
    }

    fun add(record: S) {
        addedEntries.add(record)
        deletedEntries.remove(record)
    }

    fun delete(record: S) {
        deletedEntries.add(record)
        addedEntries.remove(record)
    }

    fun abort() {
        addedEntries.clear()
        deletedEntries.clear()
    }

    abstract fun hasKey(record: S): Boolean
    abstract fun commit()
    abstract fun serializeKey(record: S): K
}
