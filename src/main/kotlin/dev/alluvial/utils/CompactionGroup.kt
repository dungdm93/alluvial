package dev.alluvial.utils

import org.apache.iceberg.Snapshot
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions.ValidationException
import java.util.function.Function

data class CompactionGroup(
    val tableId: TableIdentifier,
    val key: String,
    val size: Int,
    val lowSnapshotId: Long?,
    val lowSequenceNumber: Long?,
    val highSnapshotId: Long,
    val highSequenceNumber: Long,
) : Comparable<CompactionGroup> {
    companion object {
        fun builderFor(tableId: TableIdentifier): Builder {
            return Builder(tableId)
        }

        fun fromSnapshots(
            tableId: TableIdentifier,
            snapshots: Iterable<Snapshot>,
            keyExtractor: Function<Snapshot, String>
        ): Iterable<CompactionGroup> {
            return CompactionGroupIterable(tableId, snapshots, keyExtractor)
        }
    }

    override fun compareTo(other: CompactionGroup): Int {
        return this.highSequenceNumber.compareTo(other.highSequenceNumber)
    }

    override fun toString(): String {
        return "CompactGroup($tableId range=($lowSnapshotId..$highSnapshotId], key=$key, size=$size)"
    }

    data class Builder(val tableId: TableIdentifier) {
        var key: String = ""
        var size: Int = 0
        var lowSnapshotId: Long? = null
        var lowSequenceNumber: Long? = null
        var highSnapshotId: Long? = null
        var highSequenceNumber: Long? = null

        fun build(): CompactionGroup {
            val highSnapshotId = this.highSnapshotId
                ?: throw ValidationException("highSnapshotId is required")
            val highSequenceNumber = this.highSequenceNumber
                ?: throw ValidationException("highSequenceNumber is required")

            return CompactionGroup(
                tableId, key, size,
                lowSnapshotId, lowSequenceNumber,
                highSnapshotId, highSequenceNumber,
            )
        }

        fun clear() {
            key = ""
            size = 0
            lowSnapshotId = null
            lowSequenceNumber = null
            highSnapshotId = null
            highSequenceNumber = null
        }
    }

    class CompactionGroupIterable(
        private val tableId: TableIdentifier,
        private val snapshots: Iterable<Snapshot>,
        private val keyExtractor: Function<Snapshot, String>
    ) : Iterable<CompactionGroup> {
        override fun iterator(): Iterator<CompactionGroup> {
            return CompactionGroupIterator(tableId, snapshots.iterator(), keyExtractor)
        }
    }

    class CompactionGroupIterator(
        tableId: TableIdentifier,
        private val snapshots: Iterator<Snapshot>,
        private val keyExtractor: Function<Snapshot, String>
    ) : Iterator<CompactionGroup> {
        private val builder = builderFor(tableId)
        private var next: CompactionGroup? = null

        override fun hasNext(): Boolean {
            return next == null || advance()
        }

        override fun next(): CompactionGroup {
            if (!hasNext()) throw NoSuchElementException()

            val cg = this.next!!
            this.next = null
            return cg
        }

        private fun advance(): Boolean {
            while (snapshots.hasNext()) {
                val snapshot = snapshots.next()
                val key = keyExtractor.apply(snapshot)

                if (key != builder.key) {
                    next = builder.build()
                    resetBuilderTo(key, snapshot)
                    return true
                }
                addSnapshotToBuilder(snapshot)
            }

            if (builder.size <= 0) return false

            addSnapshotToBuilder(null) // null mean root snapshot
            next = builder.build()
            builder.clear()

            return true
        }

        private fun resetBuilderTo(key: String, snapshot: Snapshot) {
            builder.clear()
            builder.key = key
            builder.highSnapshotId = snapshot.snapshotId()
            builder.highSequenceNumber = snapshot.sequenceNumber()
            builder.lowSnapshotId = snapshot.snapshotId()
            builder.lowSequenceNumber = snapshot.sequenceNumber()
        }

        private fun addSnapshotToBuilder(snapshot: Snapshot?) {
            builder.lowSnapshotId = snapshot?.snapshotId()
            builder.lowSequenceNumber = snapshot?.sequenceNumber()
            builder.size++
        }
    }
}
