package dev.alluvial.runtime

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import dev.alluvial.utils.DurationDeserializer
import dev.alluvial.utils.DurationSerializer
import java.time.Duration
import java.time.ZoneOffset

data class Config(
    val source: SourceConfig,
    val sink: SinkConfig,
    val stream: StreamConfig,
    val manager: ManagerConfig,
    val metrics: MetricsConfig,
    val telemetry: TelemetryConfig,
)

data class SourceConfig(
    val kind: String,
    val topicPrefix: String,
    val topicsExcluded: List<String> = emptyList(),
    val topicsIncluded: List<String> = emptyList(),
    val config: Map<String, String>,
    val namingAdjusters: List<Map<String, Any>> = emptyList(),
) {
    @Transient
    val pollTimeout: Duration

    init {
        require(kind.isNotEmpty()) { "source.kind cannot be empty" }

        val timeoutMs = config.getOrDefault("poll.timeout.ms", "1000").toLong()
        pollTimeout = Duration.ofMillis(timeoutMs)
    }
}

data class SinkConfig(
    val kind: String,
    val catalog: Map<String, String>,
    val tableCreation: TableCreationConfig,
) {
    init {
        require(kind.isNotEmpty()) { "sink.kind cannot be empty" }
    }
}

data class StreamConfig(
    val kind: String,
    val connector: String,

    @JsonSerialize(using = DurationSerializer::class)
    @JsonDeserialize(using = DurationDeserializer::class)
    val examineInterval: Duration = Duration.ofMinutes(3),

    @JsonSerialize(using = DurationSerializer::class)
    @JsonDeserialize(using = DurationDeserializer::class)
    val idleTimeout: Duration = Duration.ofMinutes(15),

    val commitBatchSize: Int = 1000,

    @JsonSerialize(using = DurationSerializer::class)
    @JsonDeserialize(using = DurationDeserializer::class)
    val commitTimespan: Duration = Duration.ofMinutes(10),

    val rotateByDateInTz: ZoneOffset = ZoneOffset.UTC,
) {
    init {
        require(kind.isNotEmpty()) { "stream.kind cannot be empty" }
        require(connector.isNotEmpty()) { "stream.connector cannot be empty" }
    }
}

data class TableCreationConfig(
    val properties: Map<String, String> = emptyMap(),
    val partitionSpec: Map<String, List<PartitionSpecConfig>> = emptyMap(),
    val baseLocation: String? = null
    // TODO: support sort order configuration
)

data class PartitionSpecConfig(
    val column: String,
    val transform: String,
    val name: String?,
) {
    init {
        require(column.isNotEmpty()) { "Source column name cannot be empty" }
        require(transform.isNotEmpty()) { "Transform type cannot be empty" }
    }
}

data class ManagerConfig(
    @Suppress("ArrayInDataClass")
    val namespace: Array<String>,

    @JsonSerialize(using = DurationSerializer::class)
    @JsonDeserialize(using = DurationDeserializer::class)
    val examineInterval: Duration = Duration.ofMinutes(30),

    val compactSnapshots: CompactionConfig = CompactionConfig(),

    val expireOrphanSnapshots: ExpirationConfig = ExpirationConfig(),
)

data class CompactionConfig(
    val tz: ZoneOffset = ZoneOffset.UTC,

    /**
     * Number of hours snapshots will be kept as it is.
     * Snapshots after this point will be compacted.
     */
    val retainRaw: Long = 3,

    /**
     * Number of hours that HourCompact snapshots will be kept at least.
     * After that, it can be compacted to Day range.
     */
    val retainHourCompact: Long = 24,

    /**
     * Number of days that DayCompact snapshots will be kept at least.
     * After that, it can be compacted to Month range.
     */
    val retainDayCompact: Long = 30,

    /**
     * Number of months that MonthCompact snapshots will be kept at least.
     * After that, it can be compacted to Year range.
     */
    val retainMonthCompact: Long = 12,

    // val keepYearCompact: Int,
)

data class ExpirationConfig(
    val enabled: Boolean = true,

    @JsonSerialize(using = DurationSerializer::class)
    @JsonDeserialize(using = DurationDeserializer::class)
    val age: Duration = Duration.ofDays(1),
)

data class MetricsConfig(
    val exporters: List<MetricsExporterConfig> = emptyList(),
    val commonTags: Map<String, String> = emptyMap(),
    // TODO: Support including/excluding meters by patterns
)

data class MetricsExporterConfig(
    val kind: String,
    val properties: Map<String, String> = emptyMap(),
) {
    init {
        require(kind.isNotEmpty()) { "metrics.exporters.kind cannot be empty" }
    }
}

data class TelemetryConfig(
    val enabled: Boolean = false,
    val properties: Map<String, String> = emptyMap(),
)
