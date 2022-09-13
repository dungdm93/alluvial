package dev.alluvial.source.kafka

import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.sink.SinkRecord

fun KafkaSchema.fieldSchema(fieldName: String): KafkaSchema {
    return this.field(fieldName).schema()
}

inline fun structSchema(version: Int = 1, block: SchemaBuilder.() -> Unit): KafkaSchema {
    return SchemaBuilder.struct().also(block).version(version).build()
}

fun KafkaStruct.sourceTimestamp(): Long? {
    val source = (this["source"] as KafkaStruct?) ?: return null
    return source["ts_ms"] as Long?
}

fun SinkRecord.sourceTimestamp(): Long? {
    val value = (this.value() as KafkaStruct?) ?: return null
    return value.sourceTimestamp()
}
