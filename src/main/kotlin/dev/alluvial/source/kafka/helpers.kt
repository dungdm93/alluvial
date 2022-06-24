package dev.alluvial.source.kafka

import dev.alluvial.sink.iceberg.type.KafkaSchema
import org.apache.kafka.connect.data.SchemaBuilder

fun KafkaSchema.fieldSchema(fieldName: String): KafkaSchema {
    return this.field(fieldName).schema()
}

inline fun structSchema(version: Int = 1, block: SchemaBuilder.() -> Unit): KafkaSchema {
    return SchemaBuilder.struct().also(block).version(version).build()
}
