package dev.alluvial.utils

import org.apache.iceberg.Snapshot
import org.apache.iceberg.catalog.TableIdentifier
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

    class Builder(
        val tableId: TableIdentifier,
        val key: String,
        snapshot: Snapshot
    ) {
        private val highSnapshotId: Long = snapshot.snapshotId()
        private val highSequenceNumber: Long = snapshot.sequenceNumber()
        private var lowSnapshotId: Long? = snapshot.snapshotId()
        private var lowSequenceNumber: Long? = snapshot.sequenceNumber()
        private var parentSnapshotId: Long? = snapshot.parentId()
        var size: Int = 0

        fun moveLowWatermarkTo(snapshot: Snapshot?): Builder {
            assert(snapshot == null || parentSnapshotId == snapshot.snapshotId())

            lowSnapshotId = snapshot?.snapshotId()
            lowSequenceNumber = snapshot?.sequenceNumber()
            parentSnapshotId = snapshot?.parentId()
            size++

            return this
        }

        fun build(): CompactionGroup {
            return CompactionGroup(
                tableId, key, size,
                lowSnapshotId, lowSequenceNumber,
                highSnapshotId, highSequenceNumber,
            )
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
        private val tableId: TableIdentifier,
        private val snapshots: Iterator<Snapshot>,
        private val keyExtractor: Function<Snapshot, String>
    ) : Iterator<CompactionGroup> {
        private var builder: Builder? = null
        private var next: CompactionGroup? = null

        override fun hasNext(): Boolean {
            return next != null || advance()
        }

        override fun next(): CompactionGroup {
            if (!hasNext()) throw NoSuchElementException()

            val cg = this.next!!
            this.next = null
            return cg
        }

        private fun advance(): Boolean {
            var builder: Builder? = this.builder
            while (snapshots.hasNext()) {
                val snapshot = snapshots.next()
                val key = keyExtractor.apply(snapshot)

                when {
                    builder == null -> builder = Builder(tableId, key, snapshot)

                    builder.key != key -> {
                        next = builder.moveLowWatermarkTo(snapshot).build()
                        this.builder = Builder(tableId, key, snapshot)
                        return true
                    }

                    else -> builder.moveLowWatermarkTo(snapshot)
                }
            }

            if (builder == null)
                return false

            next = builder.moveLowWatermarkTo(null).build()
            this.builder = null
            return true
        }
    }
}
