package dev.alluvial.sink.iceberg.io

import dev.alluvial.sink.iceberg.data.RandomKafkaStruct
import dev.alluvial.sink.iceberg.data.toIcebergSchema
import dev.alluvial.source.kafka.DEBEZIUM_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_PRIMITIVES_SCHEMA
import org.apache.iceberg.StructLike
import org.apache.iceberg.types.Type.TypeID.*
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.jupiter.api.Test
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

class TestStructWrapper {
    private fun wrapAndGet(sSchema: KafkaSchema) {
        val iSchema = sSchema.toIcebergSchema()
        val wrapper = StructWrapper(sSchema, iSchema)

        val sStructs = RandomKafkaStruct.generate(sSchema, 100, 0L)

        sStructs.forEach { sStruct ->
            wrapper.wrap(sStruct)
            validate(sStruct, wrapper, sSchema, iSchema)
        }
    }

    private fun validate(
        sStruct: KafkaStruct, wrapper: StructWrapper,
        sSchema: KafkaSchema, iSchema: IcebergSchema
    ) {
        val fields = iSchema.columns()
        fields.forEachIndexed { i, field ->
            when (val fieldTypeId = field.type().typeId()) {
                LIST -> {
                    val iValue = wrapper.get(i, List::class.java)
                    val sValue = sStruct[field.name()] as List<*>?
                }
                MAP -> {
                    val iValue = wrapper.get(i, Map::class.java)
                    val sValue = sStruct[field.name()] as Map<*, *>?
                }
                STRUCT -> {
                    val iValue = wrapper.get(i, StructLike::class.java)
                    val sValue = sStruct[field.name()] as KafkaStruct?
                }
                else -> {
                    val iValue = wrapper.get(i, fieldTypeId.javaClass())
                    val sValue = sStruct[field.name()]
                }
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
