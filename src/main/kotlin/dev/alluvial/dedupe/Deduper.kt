package dev.alluvial.dedupe

interface Deduper<R> {
    fun add(record: R)
    fun remove(record: R)
    fun contains(record: R): Boolean
    fun clear()
    fun commit()
    fun abort()
}
