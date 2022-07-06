@file:Suppress("unused", "NOTHING_TO_INLINE")

package dev.alluvial.sink.iceberg

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import dev.alluvial.sink.iceberg.type.IcebergTable
import org.apache.iceberg.SnapshotSummary.EXTRA_METADATA_PREFIX
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.CloseableIterator
import org.apache.iceberg.relocated.com.google.common.collect.Iterables
import org.apache.iceberg.relocated.com.google.common.collect.Iterators
import org.apache.iceberg.relocated.com.google.common.collect.PeekingIterator

internal val mapper = JsonMapper()
internal val offsetsTypeRef = object : TypeReference<Map<Int, Long>>() {}
internal const val ALLUVIAL_POSITION_PROP = EXTRA_METADATA_PREFIX + "alluvial.position"
internal const val ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP = EXTRA_METADATA_PREFIX + "alluvial.last-record.timestamp"

fun IcebergTable.committedOffsets(): Map<Int, Long> {
    val serialized = this.currentSnapshot()
        ?.summary()
        ?.get(ALLUVIAL_POSITION_PROP)
        ?: return emptyMap()
    return mapper.readValue(serialized, offsetsTypeRef)
}

/////////////////// CloseableIterable ///////////////////
inline fun <T> Iterable<CloseableIterable<T>>.concat(): CloseableIterable<T> {
    return CloseableIterable.concat(this)
}

inline fun <T> CloseableIterable<T>.filter(crossinline predicate: (T) -> Boolean): CloseableIterable<T> {
    return CloseableIterable.filter(this) { predicate(it!!) }
}

inline fun <F, T> CloseableIterable<F>.transform(crossinline function: (F) -> T): CloseableIterable<T> {
    return CloseableIterable.transform(this) { function(it!!) }
}

/////////////////// CloseableIterator ///////////////////
inline fun <F, T> CloseableIterator<F>.transform(crossinline function: (F) -> T): CloseableIterator<T> {
    return CloseableIterator.transform(this) { function(it!!) }
}

/////////////////// Iterable ///////////////////
inline fun <T> Iterable<T>.cycle(): Iterable<T> {
    return Iterables.cycle(this)
}

inline fun <T> Iterable<Iterable<T>>.concat(): Iterable<T> {
    return Iterables.concat(this)
}

inline fun <T> Iterable<T>.concat(vararg inputs: Iterable<T>): Iterable<T> {
    return Iterables.concat(this, *inputs)
}

inline fun <T> Iterable<T>.partition(size: Int): Iterable<List<T>> {
    return Iterables.partition(this, size)
}

inline fun <T> Iterable<T>.paddedPartition(size: Int): Iterable<List<T>> {
    return Iterables.paddedPartition(this, size) as Iterable<List<T>>
}

inline fun <T> Iterable<T>.filter(crossinline predicate: (T) -> Boolean): Iterable<T> {
    return Iterables.filter(this) { predicate(it!!) }
}

inline fun <T> Iterable<*>.filter(type: Class<T>): Iterable<T> {
    return Iterables.filter(this, type)
}

inline fun <F, T> Iterable<F>.transform(crossinline function: (F) -> T): Iterable<T> {
    return Iterables.transform(this) { function(it!!) }
}

inline fun <T> Iterable<T>.skip(numberToSkip: Int): Iterable<T> {
    return Iterables.skip(this, numberToSkip)
}

inline fun <T> Iterable<T>.limit(limitSize: Int): Iterable<T> {
    return Iterables.limit(this, limitSize)
}

/////////////////// Iterator ///////////////////
inline fun <T> Iterator<Iterator<T>>.concat(): Iterator<T> {
    return Iterators.concat(this)
}

inline fun <T> Iterator<T>.concat(vararg inputs: Iterator<T>): Iterator<T> {
    return Iterators.concat(this, *inputs)
}

inline fun <T> Iterator<T>.partition(size: Int): Iterator<List<T>> {
    return Iterators.partition(this, size)
}

inline fun <T> Iterator<T>.paddedPartition(size: Int): Iterator<List<T>> {
    return Iterators.paddedPartition(this, size) as Iterator<List<T>>
}

inline fun <T> Iterator<T>.filter(crossinline predicate: (T) -> Boolean): Iterator<T> {
    return Iterators.filter(this) { predicate(it!!) }
}

inline fun <T> Iterator<*>.filter(type: Class<T>): Iterator<T> {
    return Iterators.filter(this, type)
}

inline fun <F, T> Iterator<F>.transform(crossinline function: (F) -> T): Iterator<T> {
    return Iterators.transform(this) { function(it!!) }
}

inline fun <T> Iterator<T>.limit(limitSize: Int): Iterator<T> {
    return Iterators.limit(this, limitSize)
}

inline fun <T> Iterator<T>.toPeekingIterator(): PeekingIterator<T> {
    return Iterators.peekingIterator(this)
}
