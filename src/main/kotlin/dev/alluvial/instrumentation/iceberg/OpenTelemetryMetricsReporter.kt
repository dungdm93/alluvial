package dev.alluvial.instrumentation.iceberg

import dev.alluvial.utils.Units.BYTES
import dev.alluvial.utils.merge
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongHistogram
import io.opentelemetry.api.metrics.Meter
import org.apache.iceberg.FileContent
import org.apache.iceberg.FileContent.*
import org.apache.iceberg.metrics.CommitReport
import org.apache.iceberg.metrics.MetricsReport
import org.apache.iceberg.metrics.MetricsReporter
import org.apache.iceberg.metrics.ScanReport
import org.slf4j.LoggerFactory

class OpenTelemetryMetricsReporter : MetricsReporter {
    companion object {
        private val logger = LoggerFactory.getLogger(OpenTelemetryMetricsReporter::class.java)

        private val ADDED_OPERATION_ATTRIBUTES = Attributes.of(stringKey("op"), "added")
        private val REMOVED_OPERATION_ATTRIBUTES = Attributes.of(stringKey("op"), "removed")
        private val FILE_CONTENT_ATTRIBUTES = FileContent.entries
            .associateWith {
                Attributes.of(stringKey("content"), it.name.lowercase())
            }
    }

    private lateinit var meter: Meter

    private lateinit var scanTotal: LongCounter
    private lateinit var scanPlanningDuration: LongHistogram

    private lateinit var commitTotal: LongCounter
    private lateinit var commitDuration: LongHistogram
    private lateinit var commitAttempts: LongCounter
    private lateinit var commitFiles: LongCounter
    private lateinit var commitRecords: LongCounter
    private lateinit var commitFileSize: LongCounter

    override fun initialize(properties: Map<String, String>) {
        val meterBuilder = GlobalOpenTelemetry.meterBuilder("")
        meter = meterBuilder.build()

        scanTotal = meter.counterBuilder("iceberg.scan.total").build()
        scanPlanningDuration = meter.histogramBuilder("iceberg.scan.planning.duration").ofLongs().build()

        commitTotal = meter.counterBuilder("iceberg.commit.total").build()
        commitDuration = meter.histogramBuilder("iceberg.commit.duration").ofLongs().build()
        commitAttempts = meter.counterBuilder("iceberg.commit.attempts").build()
        commitFiles = meter.counterBuilder("iceberg.commit.files").build()
        commitRecords = meter.counterBuilder("iceberg.commit.records").build()
        commitFileSize = meter.counterBuilder("iceberg.commit.file_size").setUnit(BYTES).build()
    }

    override fun report(report: MetricsReport) {
        when (report) {
            is ScanReport -> reportScanReport(report)
            is CommitReport -> reportCommitReport(report)
            else -> logger.warn("Unsupported report type ${report::class}")
        }
    }

    private fun reportScanReport(report: ScanReport) {
        val table = Attributes.of(stringKey("table"), report.tableName())
        val smr = report.scanMetrics()

        scanTotal.add(1, table)
        smr.totalPlanningDuration()?.let { scanPlanningDuration.record(it.totalDuration().toNanos(), table) }
    }

    private fun reportCommitReport(report: CommitReport) {
        val table = Attributes.of(stringKey("table"), report.tableName())
        val cmr = report.commitMetrics()

        commitTotal.add(1, table)
        cmr.totalDuration()?.let { commitDuration.record(it.totalDuration().toNanos(), table) }
        cmr.attempts()?.let { commitAttempts.add(it.value(), table) }

        // Added Files
        cmr.addedDataFiles()?.let {
            val attrs = table.merge(ADDED_OPERATION_ATTRIBUTES, FILE_CONTENT_ATTRIBUTES[DATA]!!)
            commitFiles.add(it.value(), attrs)
        }
        cmr.addedPositionalDeleteFiles()?.let {
            val attrs = table.merge(ADDED_OPERATION_ATTRIBUTES, FILE_CONTENT_ATTRIBUTES[POSITION_DELETES]!!)
            commitFiles.add(it.value(), attrs)
        }
        cmr.addedEqualityDeleteFiles()?.let {
            val attrs = table.merge(ADDED_OPERATION_ATTRIBUTES, FILE_CONTENT_ATTRIBUTES[EQUALITY_DELETES]!!)
            commitFiles.add(it.value(), attrs)
        }

        // Removed Files
        cmr.removedDataFiles()?.let {
            val attrs = table.merge(REMOVED_OPERATION_ATTRIBUTES, FILE_CONTENT_ATTRIBUTES[DATA]!!)
            commitFiles.add(it.value(), attrs)
        }
        cmr.removedPositionalDeleteFiles()?.let {
            val attrs = table.merge(REMOVED_OPERATION_ATTRIBUTES, FILE_CONTENT_ATTRIBUTES[POSITION_DELETES]!!)
            commitFiles.add(it.value(), attrs)
        }
        cmr.removedEqualityDeleteFiles()?.let {
            val attrs = table.merge(REMOVED_OPERATION_ATTRIBUTES, FILE_CONTENT_ATTRIBUTES[EQUALITY_DELETES]!!)
            commitFiles.add(it.value(), attrs)
        }

        // Records
        cmr.addedRecords()?.let {
            val attrs = table.merge(ADDED_OPERATION_ATTRIBUTES)
            commitRecords.add(it.value(), attrs)
        }
        cmr.removedRecords()?.let {
            val attrs = table.merge(REMOVED_OPERATION_ATTRIBUTES)
            commitRecords.add(it.value(), attrs)
        }

        // File size
        cmr.addedFilesSizeInBytes()?.let {
            val attrs = table.merge(ADDED_OPERATION_ATTRIBUTES)
            commitFileSize.add(it.value(), attrs)
        }
        cmr.removedFilesSizeInBytes()?.let {
            val attrs = table.merge(REMOVED_OPERATION_ATTRIBUTES)
            commitFileSize.add(it.value(), attrs)
        }
    }
}
