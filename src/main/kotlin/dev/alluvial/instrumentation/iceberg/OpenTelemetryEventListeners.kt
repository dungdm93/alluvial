package dev.alluvial.instrumentation.iceberg

import dev.alluvial.utils.ICEBERG_OPERATION
import dev.alluvial.utils.ICEBERG_SEQUENCE_NUMBER
import dev.alluvial.utils.ICEBERG_SNAPSHOT_ID
import dev.alluvial.utils.ICEBERG_TABLE
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import org.apache.iceberg.events.CreateSnapshotEvent
import org.apache.iceberg.events.IncrementalScanEvent
import org.apache.iceberg.events.Listener
import org.apache.iceberg.events.Listeners
import org.apache.iceberg.events.ScanEvent

object OpenTelemetryEventListeners {
    private object OnSnapshotCreated : Listener<CreateSnapshotEvent> {
        override fun notify(event: CreateSnapshotEvent) {
            val span = Span.current()
            val attrs = Attributes.builder()
                .put(ICEBERG_TABLE, event.tableName())
                .put(ICEBERG_OPERATION, event.operation())
                .put(ICEBERG_SNAPSHOT_ID, event.snapshotId())
                .put(ICEBERG_SEQUENCE_NUMBER, event.sequenceNumber())
                .build()

            span.addEvent("Iceberg.COMMIT", attrs)
        }
    }

    private object OnScan : Listener<ScanEvent> {
        override fun notify(event: ScanEvent) {
            val span = Span.current()
            val attrs = Attributes.builder()
                .put(ICEBERG_TABLE, event.tableName())
                .put(ICEBERG_SNAPSHOT_ID, event.snapshotId())
                .build()

            span.addEvent("Iceberg.SCAN", attrs)
        }
    }

    private object OnIncrementalScan : Listener<IncrementalScanEvent> {
        override fun notify(event: IncrementalScanEvent) {
            val span = Span.current()
            val attrs = Attributes.builder()
                .put(ICEBERG_TABLE, event.tableName())
                .put(AttributeKey.longKey("iceberg.fromSnapshotId"), event.fromSnapshotId())
                .put(AttributeKey.longKey("iceberg.toSnapshotId"), event.toSnapshotId())
                .build()

            span.addEvent("Iceberg.INCREMENTAL_SCAN", attrs)
        }
    }

    private var registered = false

    fun register() {
        if (registered) return

        Listeners.register(OnScan, ScanEvent::class.java)
        Listeners.register(OnIncrementalScan, IncrementalScanEvent::class.java)
        Listeners.register(OnSnapshotCreated, CreateSnapshotEvent::class.java)
        registered = true
    }
}
