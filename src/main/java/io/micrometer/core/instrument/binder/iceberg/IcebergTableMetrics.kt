package io.micrometer.core.instrument.binder.iceberg

import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.util.NamedThreadFactory
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.Snapshot
import org.apache.iceberg.SnapshotSummary.*
import org.apache.iceberg.Table
import org.apache.iceberg.currentAncestors
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.ToDoubleFunction

class IcebergTableMetrics(
    private val table: Table,
    tags: Iterable<Tag> = emptyList(),
    private val refreshInterval: Duration = Duration.ofMinutes(1),
) : MeterBinder, AutoCloseable {
    private val ops = (table as HasTableOperations).operations()
    private val tags = Tags.concat(tags, "table", table.name())
    private val meters = mutableListOf<Meter>()
    private lateinit var registry: MeterRegistry

    override fun bindTo(registry: MeterRegistry) {
        this.registry = registry

        createAndBindMeter()

        val refreshIntervalMs = refreshInterval.toMillis()
        scheduler.scheduleWithFixedDelay(table::refresh, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS)
    }

    override fun close() {
        meters.forEach {
            registry.remove(it)
            it.close()
        }
    }

    private fun createAndBindMeter() {
        ///// Metadata /////
        registerCounter("meta.lastUpdatedMillis", "lastSequenceNumber of table", ops) {
            it.current().lastUpdatedMillis().toDouble()
        }
        registerCounter("meta.lastSequenceNumber", "lastSequenceNumber of table", ops) {
            it.current().lastSequenceNumber().toDouble()
        }
        registerCounter("meta.lastColumnId", "lastColumnId", ops) {
            it.current().lastColumnId().toDouble()
        }
        registerCounter("meta.schemas.total", "total schema version in the history", ops) {
            it.current().schemas().count().toDouble()
        }
        registerCounter("meta.schemas.current", "current schema id of table", ops) {
            it.current().currentSchemaId().toDouble()
        }
        registerCounter("meta.specs.total", "total partition specs in the history", ops) {
            it.current().specs().count().toDouble()
        }
        registerGauge("meta.specs.default", "default partition spec id of the table", ops) {
            it.current().defaultSpecId().toDouble()
        }
        registerCounter("meta.sortOrder.total", "total sortOrders in the history", ops) {
            it.current().sortOrders().count().toDouble()
        }
        registerGauge("meta.sortOrder.default", "default sortOrder id of the table", ops) {
            it.current().defaultSortOrderId().toDouble()
        }

        ///// References /////
        registerGauge("meta.refs.total", "total references (branches + tags) of the table", ops) {
            it.current().refs().count().toDouble()
        }

        ///// Snapshots /////
        registerGauge("snapshots.timeline.total", "Total snapshots in main branch timeline", table) {
            it.currentAncestors().count().toDouble()
        }
        registerGauge("snapshots.history.total", "Total snapshots in history", table) {
            it.snapshots().count().toDouble()
        }

        ///// Files /////
        registerGauge(
            "files.timeline.total", "Total DATA files in main branch timeline",
            Tags.of("content", "data"), table
        ) { it.currentSnapshotSummaryAsDouble(TOTAL_DATA_FILES_PROP, ZERO) }
        registerGauge(
            "files.timeline.total", "Total DELETES (POSITION_DELETES + EQUALITY_DELETES) files in main branch timeline",
            Tags.of("content", "deletes"), table
        ) { it.currentSnapshotSummaryAsDouble(TOTAL_DELETE_FILES_PROP, ZERO) }
        //registerGauge(
        //    "files.history.total", "Total DATA files in history",
        //    Tags.of("content", "data"), table
        //) { it.totalSnapshotSummaryAsDouble(ADDED_FILES_PROP, ZERO) }
        //registerGauge(
        //    "files.history.total", "Total POSITION_DELETES files in history",
        //    Tags.of("content", "position-deletes"), table
        //) { it.totalSnapshotSummaryAsDouble(ADD_EQ_DELETE_FILES_PROP, ZERO) }
        //registerGauge(
        //    "files.history.total", "Total EQUALITY_DELETES files in history",
        //    Tags.of("content", "equality-deletes"), table
        //) { it.totalSnapshotSummaryAsDouble(ADD_POS_DELETE_FILES_PROP, ZERO) }

        ///// Records /////
        registerGauge(
            "records.timeline.total", "Total DATA records in main branch timeline",
            Tags.of("content", "data"), table
        ) { it.currentSnapshotSummaryAsDouble(TOTAL_RECORDS_PROP, ZERO) }
        registerGauge(
            "records.timeline.total", "Total POSITION_DELETES records in main branch timeline",
            Tags.of("content", "position-deletes"), table
        ) { it.currentSnapshotSummaryAsDouble(TOTAL_POS_DELETES_PROP, ZERO) }
        registerGauge(
            "records.timeline.total", "Total EQUALITY_DELETES records in main branch timeline",
            Tags.of("content", "equality-deletes"), table
        ) { it.currentSnapshotSummaryAsDouble(TOTAL_EQ_DELETES_PROP, ZERO) }
        //registerGauge(
        //    "records.history.total", "Total DATA records in history",
        //    Tags.of("content", "data"), table
        //) { it.totalSnapshotSummaryAsDouble(ADDED_RECORDS_PROP, ZERO) }
        //registerGauge(
        //    "records.history.total", "Total POSITION_DELETES records in history",
        //    Tags.of("content", "position-deletes"), table
        //) { it.totalSnapshotSummaryAsDouble(ADDED_POS_DELETES_PROP, ZERO) }
        //registerGauge(
        //    "records.history.total", "Total EQUALITY_DELETES records in history",
        //    Tags.of("content", "equality-deletes"), table
        //) { it.totalSnapshotSummaryAsDouble(ADDED_EQ_DELETES_PROP, ZERO) }

        ///// Files Size /////
        registerGauge("files_size.timeline.total", "Total files size in main branch timeline", table) {
            it.currentSnapshotSummaryAsDouble(TOTAL_FILE_SIZE_PROP, ZERO)
        }
        //registerGauge("files_size.history.total", "Total files size in history", table) {
        //    it.totalSnapshotSummaryAsDouble(ADDED_FILE_SIZE_PROP, ZERO)
        //}
    }

    private fun <O> registerGauge(name: String, desc: String, obj: O, func: ToDoubleFunction<O>) {
        registerGauge(name, desc, Tags.empty(), obj, func)
    }

    private fun <O> registerGauge(name: String, desc: String, extraTags: Tags, obj: O, func: ToDoubleFunction<O>) {
        val meter = Gauge.builder(METRIC_NAME_PREFIX + name, obj, func)
            .description(desc)
            .tags(tags)
            .tags(extraTags)
            .register(registry)

        meters.add(meter)
    }

    private fun <O> registerCounter(name: String, desc: String, obj: O, func: ToDoubleFunction<O>) {
        registerCounter(name, desc, Tags.empty(), obj, func)
    }

    private fun <O> registerCounter(name: String, desc: String, extraTags: Tags, obj: O, func: ToDoubleFunction<O>) {
        val meter = FunctionCounter.builder(METRIC_NAME_PREFIX + name, obj, func)
            .description(desc)
            .tags(tags)
            .tags(extraTags)
            .register(registry)

        meters.add(meter)
    }

    companion object {
        private val scheduler = Executors.newScheduledThreadPool(3, NamedThreadFactory("micrometer-iceberg-metrics"))
        private const val ZERO: Double = 0.0
        const val METRIC_NAME_PREFIX = "iceberg.table."

        private fun Snapshot.summaryAsDouble(prop: String, default: Double): Double {
            val value = this.summary()[prop] ?: return default
            return value.toDouble()
        }

        private fun Table.currentSnapshotSummaryAsDouble(prop: String, default: Double): Double {
            val snapshot = this.currentSnapshot() ?: return default
            return snapshot.summaryAsDouble(prop, default)
        }

        private fun Table.totalSnapshotSummaryAsDouble(prop: String, default: Double): Double {
            return this.snapshots().sumOf { it.summaryAsDouble(prop, default) }
        }
    }
}
