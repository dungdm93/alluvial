package dev.alluvial.sink.iceberg.io

import dev.alluvial.sink.iceberg.data.KafkaTypeToType
import dev.alluvial.sink.iceberg.data.RandomKafkaStruct
import dev.alluvial.sink.iceberg.data.toIcebergSchema
import dev.alluvial.source.kafka.DEBEZIUM_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_PRIMITIVES_SCHEMA
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.jupiter.api.Test
import strikt.api.expectThat

class TestStructWrapper {
    private fun wrapAndGet(schema: Schema) {
        val kafkaStructs = RandomKafkaStruct.generate(schema, 100, 0L).toList()
        val wrapper = StructWrapper(schema.toIcebergSchema())
        kafkaStructs.forEach { struct ->
            wrapper.wrap(struct)
            val fields = struct.schema().fields()
            fields.filter { field ->
                field.schema().type().isPrimitive
            }.forEach { field ->
                val icebergTypeID = KafkaTypeToType().primitive(field.schema()).typeId()
                expectThat(wrapper.get(field.index(), icebergTypeID.javaClass()))
            }
        }
    }

    @Test
    fun testKafkaPrimitives() {
        val kafkaSchema = SchemaBuilder.struct().apply {
            KAFKA_PRIMITIVES_SCHEMA.forEach(::field)
        }.build()
        wrapAndGet(kafkaSchema)
    }

    @Test
    fun testKafkaLogicalTypes() {
        val kafkaSchema = SchemaBuilder.struct().apply {
            KAFKA_LOGICAL_TYPES_SCHEMA.forEach(::field)
        }.build()
        wrapAndGet(kafkaSchema)
    }

    @Test
    fun testDebeziumLogicalTypes() {
        val kafkaSchema = SchemaBuilder.struct().apply {
            DEBEZIUM_LOGICAL_TYPES_SCHEMA.forEach(::field)
        }.build()
        wrapAndGet(kafkaSchema)
    }
}
