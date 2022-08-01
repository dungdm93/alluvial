package dev.alluvial.sink.iceberg

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import dev.alluvial.sink.iceberg.type.IcebergTable
import org.apache.iceberg.SnapshotSummary

internal val mapper = JsonMapper()
internal val offsetsTypeRef = object : TypeReference<Map<Int, Long>>() {}
internal const val SUMMARY_PREFIX = SnapshotSummary.EXTRA_METADATA_PREFIX
internal const val ALLUVIAL_POSITION_PROP = "${SUMMARY_PREFIX}alluvial.position"
internal const val ALLUVIAL_LAST_RECORD_TIMESTAMP_PROP = "${SUMMARY_PREFIX}alluvial.last-record.timestamp"

fun IcebergTable.committedOffsets(): Map<Int, Long> {
    val serialized = this.currentSnapshot()
        ?.summary()
        ?.get(ALLUVIAL_POSITION_PROP)
        ?: return emptyMap()
    return mapper.readValue(serialized, offsetsTypeRef)
}
