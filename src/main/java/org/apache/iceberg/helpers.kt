package org.apache.iceberg

import org.apache.iceberg.SnapshotSummary.EXTRA_METADATA_PREFIX
import org.apache.iceberg.io.DeleteSchemaUtil
import org.apache.iceberg.types.Types
import org.apache.iceberg.util.PropertyUtil
import org.apache.iceberg.util.SnapshotUtil

fun PartitionSpec.Builder.addSpec(field: Types.NestedField, transform: String): PartitionSpec.Builder {
    return this.addSpec(field, transform, null)
}

fun PartitionSpec.Builder.addSpec(field: Types.NestedField, transform: String, name: String?): PartitionSpec.Builder {
    val targetName = if (name.isNullOrEmpty()) {
        if (transform.contains("[")) {
            field.name() + "_" + transform.substring(0, transform.indexOf('['))
        } else {
            field.name() + "_" + transform
        }
    } else name
    return this.add(field.fieldId(), targetName.lowercase(), transform)
}

/////////////////// Table ///////////////////
fun Table.currentSnapshotId(): Long? {
    return this.currentSnapshot()?.snapshotId()
}

fun Table.currentAncestors(): Iterable<Snapshot> {
    return SnapshotUtil.currentAncestors(this)
}

fun Table.currentAncestorIds(): List<Long> {
    return SnapshotUtil.currentAncestorIds(this)
}

fun Table.ancestorsOf(snapshot: Snapshot): Iterable<Snapshot> {
    return SnapshotUtil.ancestorsOf(snapshot.snapshotId(), this::snapshot)
}

fun Table.ancestorsOf(snapshotId: Long): Iterable<Snapshot> {
    return SnapshotUtil.ancestorsOf(snapshotId, this::snapshot)
}

/////////////////// TableMetadata ///////////////////
fun TableMetadata.currentSnapshotId(): Long? {
    return this.currentSnapshot()?.snapshotId()
}

fun TableMetadata.currentAncestors(): Iterable<Snapshot> {
    val currentSnapshotId = this.currentSnapshotId()
    return if (currentSnapshotId == null) {
        emptyList()
    } else {
        SnapshotUtil.ancestorsOf(currentSnapshotId, this::snapshot)
    }
}

fun TableMetadata.currentAncestorIds(): List<Long> {
    return SnapshotUtil.ancestorIds(this.currentSnapshot(), this::snapshot)
}

fun TableMetadata.ancestorsOf(snapshot: Snapshot): Iterable<Snapshot> {
    return SnapshotUtil.ancestorsOf(snapshot.snapshotId(), this::snapshot)
}

fun TableMetadata.ancestorsOf(snapshotId: Long): Iterable<Snapshot> {
    return SnapshotUtil.ancestorsOf(snapshotId, this::snapshot)
}

fun TableMetadata.isFastForward(snapshot: Snapshot): Boolean {
    return snapshot.parentId() == this.currentSnapshotId()
}

/////////////////// TableMetadata ///////////////////
const val NOOP = "" // Special DataOperations for zero aggregate changes
const val SQUASH_SNAPSHOT_ID_PROP = "squash-snapshot-id"
const val ORIGINAL_SNAPSHOT_TS_PROP = "original-snapshot-ts"
val POS_DELETE_SCHEMA: Schema = DeleteSchemaUtil.pathPosSchema()

fun Snapshot.originalTimestampMillis(): Long {
    return PropertyUtil.propertyAsLong(summary(), ORIGINAL_SNAPSHOT_TS_PROP, timestampMillis())
}

fun Snapshot.extraMetadata(): Map<String, String> {
    return this.summary()
        .filterKeys { it.startsWith(EXTRA_METADATA_PREFIX) }
}

fun Iterable<Snapshot>.filterAfter(snapshot: Snapshot?): Iterable<Snapshot> {
    if (snapshot == null) return this
    return this.filter { it.sequenceNumber() > snapshot.sequenceNumber() }
}

fun Snapshot.isAfter(snapshot: Snapshot?): Boolean {
    if (snapshot == null) return true
    return this.sequenceNumber() > snapshot.sequenceNumber()
}
