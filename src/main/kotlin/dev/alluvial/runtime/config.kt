package dev.alluvial.runtime

data class Config(
    val source: SourceConfig,
    val sink: SinkConfig,
    val stream: StreamConfig,
)

data class SourceConfig(
    val kind: String,
    val topicPrefix: String,
    val config: Map<String, String>,
) {
    init {
        require(kind.isNotEmpty()) { "source.kind cannot be empty" }
    }
}

data class SinkConfig(
    val kind: String,
    val catalog: Map<String, String>,
) {
    init {
        require(kind.isNotEmpty()) { "sink.kind cannot be empty" }
    }
}

data class StreamConfig(
    val kind: String,
    val props: Map<String, String>,
) {
    init {
        require(kind.isNotEmpty()) { "stream.kind cannot be empty" }
    }
}
