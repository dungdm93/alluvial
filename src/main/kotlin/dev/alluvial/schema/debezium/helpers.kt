package dev.alluvial.schema.debezium

import org.apache.kafka.connect.sink.SinkRecord

internal const val SCHEMA_VERSION_PROP = "alluvial.schema.version"

fun SinkRecord.schemaVersion() = "key=${keySchema().version()},value=${valueSchema().version()}"
