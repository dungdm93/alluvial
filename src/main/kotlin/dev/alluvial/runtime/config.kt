package dev.alluvial.runtime

data class Config(
    val source: ConfigSource,
    val sink: ConfigSink,
    val stream: ConfigStream,
)

data class ConfigSource(
    val kind: String,
    val props: Map<String, String>,
) {
    init {
        require(kind.isNotEmpty()) { "source.kind cannot be empty" }
    }
}

data class ConfigSink(
    val kind: String,
    val props: Map<String, String>,
) {
    init {
        require(kind.isNotEmpty()) { "sink.kind cannot be empty" }
    }
}

data class ConfigStream(
    val kind: String,
    val props: Map<String, String>,
) {
    init {
        require(kind.isNotEmpty()) { "stream.kind cannot be empty" }
    }
}
