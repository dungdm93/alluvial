package dev.alluvial.sink.iceberg.data

import org.apache.iceberg.types.Types
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isSameInstanceAs
import strikt.java.propertiesAreEqualTo
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Type as IcebergType
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.SchemaBuilder as KafkaSchemaBuilder

internal class KafkaSchemaUtilTest {
    companion object {
        private val PRIMITIVE_TYPES = mapOf(
            KafkaSchema.INT8_SCHEMA to Types.IntegerType.get(),
            KafkaSchema.INT16_SCHEMA to Types.IntegerType.get(),
            KafkaSchema.INT32_SCHEMA to Types.IntegerType.get(),
            KafkaSchema.INT64_SCHEMA to Types.LongType.get(),
            KafkaSchema.FLOAT32_SCHEMA to Types.FloatType.get(),
            KafkaSchema.FLOAT64_SCHEMA to Types.DoubleType.get(),
            KafkaSchema.BOOLEAN_SCHEMA to Types.BooleanType.get(),
            KafkaSchema.STRING_SCHEMA to Types.StringType.get(),
            KafkaSchema.BYTES_SCHEMA to Types.BinaryType.get(),

            KafkaSchema.OPTIONAL_INT8_SCHEMA to Types.IntegerType.get(),
            KafkaSchema.OPTIONAL_INT16_SCHEMA to Types.IntegerType.get(),
            KafkaSchema.OPTIONAL_INT32_SCHEMA to Types.IntegerType.get(),
            KafkaSchema.OPTIONAL_INT64_SCHEMA to Types.LongType.get(),
            KafkaSchema.OPTIONAL_FLOAT32_SCHEMA to Types.FloatType.get(),
            KafkaSchema.OPTIONAL_FLOAT64_SCHEMA to Types.DoubleType.get(),
            KafkaSchema.OPTIONAL_BOOLEAN_SCHEMA to Types.BooleanType.get(),
            KafkaSchema.OPTIONAL_STRING_SCHEMA to Types.StringType.get(),
            KafkaSchema.OPTIONAL_BYTES_SCHEMA to Types.BinaryType.get(),
        )

        private val STRUCT_OF_PRIMITIVE: Pair<KafkaSchema, IcebergType>

        init {
            var idx = 0
            val kafkaStructBuilder = KafkaSchemaBuilder.struct()
            val fields = mutableListOf<Types.NestedField>()

            for ((k, v) in PRIMITIVE_TYPES) {
                val name = k.type().getName() + (if (k.isOptional) "_optional" else "")
                kafkaStructBuilder.field(name, k.schema())
                fields.add(Types.NestedField.of(idx++, k.isOptional, name, v, k.doc()))
            }

            val kafkaStruct = kafkaStructBuilder.build()
            val icebergStruct = Types.StructType.of(fields)

            STRUCT_OF_PRIMITIVE = Pair(kafkaStruct, icebergStruct)
        }

        private fun kafkaArrayOf(schema: KafkaSchema): KafkaSchema {
            return KafkaSchemaBuilder.array(schema).build()
        }

        private fun kafkaMapOf(valueSchema: KafkaSchema): KafkaSchema {
            return KafkaSchemaBuilder.map(KafkaSchema.STRING_SCHEMA, valueSchema).build()
        }

        private fun icebergListOf(isOptional: Boolean, elementType: IcebergType): IcebergType {
            return if (isOptional)
                Types.ListType.ofOptional(0, elementType) else
                Types.ListType.ofRequired(0, elementType)
        }

        private fun icebergMapOf(isOptional: Boolean, valueType: IcebergType): IcebergType {
            return if (isOptional)
                Types.MapType.ofOptional(0, 1, Types.StringType.get(), valueType) else
                Types.MapType.ofRequired(0, 1, Types.StringType.get(), valueType)
        }
    }

    @Test
    fun `convert from Kafka Primitive Types to IcebergType`() {
        PRIMITIVE_TYPES.forEach { (k, v) ->
            val c = KafkaSchemaUtil.toIcebergType(k)
            expectThat(c).isSameInstanceAs(v)
        }
    }

    @Test
    fun `convert from Kafka Array to IcebergType`() {
        val map = buildMap {
            PRIMITIVE_TYPES.forEach { (k, v) ->
                put(kafkaArrayOf(k), icebergListOf(k.isOptional, v))
            }
        }

        map.forEach { (k, v) ->
            val c = KafkaSchemaUtil.toIcebergType(k)
            expectThat(c).isEqualTo(v)
        }
    }

    @Test
    fun `convert from Kafka Map to IcebergType`() {
        val map = buildMap {
            PRIMITIVE_TYPES.forEach { (k, v) ->
                put(kafkaMapOf(k), icebergMapOf(k.isOptional, v))
            }
        }

        map.forEach { (k, v) ->
            val c = KafkaSchemaUtil.toIcebergType(k)
            expectThat(c).isEqualTo(v)
        }
    }

    @Test
    fun `convert from Kafka Struct to IcebergType`() {
        expectThat(KafkaSchemaUtil.toIcebergType(STRUCT_OF_PRIMITIVE.first))
            .isEqualTo(STRUCT_OF_PRIMITIVE.second)
    }

    @Test
    fun `convert from Kafka Struct to IcebergSchema`() {
        val iceBergSchema = IcebergSchema(STRUCT_OF_PRIMITIVE.second.asStructType().fields())
        expectThat(KafkaSchemaUtil.toIcebergSchema(STRUCT_OF_PRIMITIVE.first))
            .propertiesAreEqualTo(iceBergSchema)
    }
}
