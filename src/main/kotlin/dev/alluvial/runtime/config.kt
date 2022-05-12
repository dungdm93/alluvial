package dev.alluvial.runtime

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import dev.alluvial.utils.DurationDeserializer
import dev.alluvial.utils.DurationSerializer
import java.time.Duration

data class Config(
    val source: SourceConfig,
    val sink: SinkConfig,
    val stream: StreamConfig,
)

data class SourceConfig(
    val kind: String,
    val topicPrefix: String,
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

    @JsonSerialize(using = DurationSerializer::class)
    @JsonDeserialize(using = DurationDeserializer::class)
    val examineInterval: Duration = Duration.ofMinutes(3),

    @JsonSerialize(using = DurationSerializer::class)
    @JsonDeserialize(using = DurationDeserializer::class)
    val idleTimeout: Duration = Duration.ofMinutes(15),

    @JsonSerialize(using = DurationSerializer::class)
    @JsonDeserialize(using = DurationDeserializer::class)
    val commitTimespan: Duration = Duration.ofMinutes(10),

    val commitBatchSize: Int = 1000,
) {
    init {
        require(kind.isNotEmpty()) { "stream.kind cannot be empty" }
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
