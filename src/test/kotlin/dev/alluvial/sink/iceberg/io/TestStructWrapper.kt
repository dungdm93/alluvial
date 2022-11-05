package dev.alluvial.sink.iceberg.io

import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.RandomKafkaStruct
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import dev.alluvial.source.kafka.DEBEZIUM_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_PRIMITIVES_SCHEMA
import dev.alluvial.source.kafka.structSchema
import org.apache.iceberg.StructLike
import org.apache.iceberg.types.Type.TypeID.*
import org.junit.jupiter.api.Test

internal class TestStructWrapper {
    private fun wrapAndGet(sSchema: KafkaSchema) {
        val iSchema = sSchema.toIcebergSchema()
        val wrapper = StructWrapper(sSchema, iSchema)

        val sStructs = RandomKafkaStruct.generate(sSchema, 100, 0L)

        sStructs.forEach { sStruct ->
            wrapper.wrap(sStruct)
            validate(sStruct, wrapper, sSchema, iSchema)
        }
    }

    @Suppress("UNUSED_VARIABLE", "UNUSED_PARAMETER")
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
        val kafkaSchema = structSchema {
            KAFKA_PRIMITIVES_SCHEMA.forEach(::field)
        }
        wrapAndGet(kafkaSchema)
    }

    @Test
    fun testKafkaLogicalTypes() {
        val kafkaSchema = structSchema {
            KAFKA_LOGICAL_TYPES_SCHEMA.forEach(::field)
        }
        wrapAndGet(kafkaSchema)
    }

    @Test
    fun testDebeziumLogicalTypes() {
        val kafkaSchema = structSchema {
            DEBEZIUM_LOGICAL_TYPES_SCHEMA.forEach(::field)
        }
        wrapAndGet(kafkaSchema)
    }
}
