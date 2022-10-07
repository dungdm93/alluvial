package dev.alluvial.dedupe

interface RecordSerializer<R, V> {
    fun serialize(record: R): V
}
