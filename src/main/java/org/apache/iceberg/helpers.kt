package org.apache.iceberg

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import dev.alluvial.sink.iceberg.filter
import dev.alluvial.stream.debezium.WALPosition
import org.apache.iceberg.SnapshotSummary.EXTRA_METADATA_PREFIX
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.DeleteSchemaUtil
import org.apache.iceberg.util.SnapshotUtil

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

/////////////////// Snapshot ///////////////////
const val NOOP = "" // Special DataOperations for zero aggregate changes
const val SOURCE_TIMESTAMP_PROP = "source-ts"
const val SOURCE_WAL_POSITION_PROP = "source-wal-position"
const val BROKER_OFFSETS_PROP = "broker-offsets"
const val SQUASH_SNAPSHOTS_ID_PROP = "squash-snapshots-id"
val POS_DELETE_SCHEMA: Schema = DeleteSchemaUtil.pathPosSchema()
val mapper: JsonMapper = JsonMapper.builder()
    .addModule(KotlinModule.Builder().build())
    .addModule(JavaTimeModule())
    .build()
internal val offsetsTypeRef = object : TypeReference<Map<Int, Long>>() {}

fun Snapshot.sourceTimestampMillis(): Long? {
    return summary()[SOURCE_TIMESTAMP_PROP]?.toLong()
}

fun Snapshot.sourceWALPosition(): WALPosition? {
    val serialized = summary()[SOURCE_WAL_POSITION_PROP] ?: return null
    return mapper.readValue(serialized, WALPosition::class.java)
}

fun Snapshot.brokerOffsets(): Map<Int, Long> {
    val serialized = summary()[BROKER_OFFSETS_PROP] ?: return emptyMap()
    return mapper.readValue(serialized, offsetsTypeRef)
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

internal fun <C : ContentFile<C>> Iterable<ManifestEntry<C>>.filterEntryAfter(snapshot: Snapshot?): Iterable<ManifestEntry<C>> {
    if (snapshot == null) return this
    return this.filter { it.sequenceNumber() > snapshot.sequenceNumber() }
}

internal fun <C : ContentFile<C>> CloseableIterable<ManifestEntry<C>>.filterEntryAfter(snapshot: Snapshot?): CloseableIterable<ManifestEntry<C>> {
    if (snapshot == null) return this
    return this.filter { it.sequenceNumber() > snapshot.sequenceNumber() }
}

internal fun ManifestGroup.filterEntryAfter(snapshot: Snapshot?): ManifestGroup {
    if (snapshot == null) return this
    return this.filterManifestEntries { it.sequenceNumber() > snapshot.sequenceNumber() }
}

@Suppress("MemberVisibilityCanBePrivate")
class SnapshotBuilder(snapshot: Snapshot) {
    var sequenceNumber: Long = snapshot.sequenceNumber()
    var snapshotId: Long = snapshot.snapshotId()
    var parentId: Long? = snapshot.parentId()
    var timestampMillis: Long = snapshot.timestampMillis()
    var operation: String = snapshot.operation()
    var summary: Map<String, String> = snapshot.summary()
    var schemaId: Int? = snapshot.schemaId()
    var manifestList: String = snapshot.manifestListLocation()

    internal fun build(): Snapshot {
        return BaseSnapshot(
            sequenceNumber, snapshotId, parentId, timestampMillis,
            operation, summary, schemaId, manifestList,
        )
    }
}

fun Snapshot.copy(block: SnapshotBuilder.() -> Unit): Snapshot {
    val builder = SnapshotBuilder(this)
    builder.block()
    return builder.build()
}
