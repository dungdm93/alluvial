package dev.alluvial.schema.debezium

import org.apache.kafka.connect.sink.SinkRecord
import java.util.Objects

internal const val SCHEMA_HASH_PROP = "alluvial.schema.hash"

fun SinkRecord.schemaHash() = Objects.hash(keySchema(), valueSchema())
