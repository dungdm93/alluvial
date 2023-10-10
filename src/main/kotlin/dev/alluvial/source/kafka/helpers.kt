package dev.alluvial.source.kafka

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import dev.alluvial.sink.iceberg.type.KafkaField
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import org.apache.kafka.connect.data.Schema.Type.*
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.sink.SinkRecord

val mapper: JsonMapper = JsonMapper.builder()
    .addModule(KotlinModule.Builder().build())
    .addModule(JavaTimeModule())
    .build()

fun KafkaSchema.fieldSchema(fieldName: String): KafkaSchema {
    return this.field(fieldName).schema()
}

fun KafkaSchema.toJsonString(): String {
    val node = this.toJson()
    return mapper.writeValueAsString(node)
}

fun KafkaSchema.toJson(): ObjectNode {
    val node = mapper.createObjectNode()

    node.put("type", this.type().name)
    if (!this.name().isNullOrEmpty()) node.put("name", this.name())
    if (this.isOptional) node.put("optional", this.isOptional)
    if (this.defaultValue() != null) node.put("default", this.defaultValue().toString())
    if (this.version() != null) node.put("version", this.version())
    if (!this.doc().isNullOrEmpty()) node.put("doc", this.doc())
    if (!this.parameters().isNullOrEmpty()) {
        val params = mapper.createObjectNode()
        this.parameters().forEach(params::put)
        node.put("parameters", params)
    }
    when (this.type()) {
        ARRAY -> {
            val items = this.valueSchema().toJson()
            node.put("items", items)
        }

        MAP -> {
            val key = this.keySchema().toJson()
            node.put("key", key)
            val value = this.valueSchema().toJson()
            node.put("value", value)
        }

        STRUCT -> {
            val fields = mapper.createArrayNode()
            this.fields().forEach {
                fields.add(it.toJson())
            }
            node.put("fields", fields)
        }

        else -> {}
    }

    return node
}

fun KafkaField.toJson(): ObjectNode {
    val node = mapper.createObjectNode()

    node.put("name", this.name())
    node.put("index", this.index())
    node.put("schema", this.schema().toJson())

    return node
}

inline fun structSchema(version: Int = 1, block: SchemaBuilder.() -> Unit): KafkaSchema {
    return SchemaBuilder.struct().also(block).version(version).build()
}

fun SinkRecord.source(): KafkaStruct? {
    val value = (this.value() as KafkaStruct?) ?: return null
    return value.getStruct("source")
}

fun SinkRecord.sourceTimestamp(): Long? {
    val source = this.source() ?: return null
    return source.getInt64("ts_ms")
}

fun SinkRecord.debeziumTimestamp(): Long? {
    val value = (this.value() as KafkaStruct?) ?: return null
    return value.getInt64("ts_ms")
}

fun SinkRecord.op(): String? {
    val value = (this.value() as KafkaStruct?) ?: return null
    return value.getString("op")
}
