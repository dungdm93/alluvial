package dev.alluvial.dedupe.backend.rocksdb

import com.google.common.base.Preconditions
import io.confluent.connect.avro.AvroData
import io.confluent.connect.avro.AvroDataConfig
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG
import io.confluent.kafka.serializers.AvroData.addLogicalTypeConversion
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectDatumWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.sink.SinkRecord
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

/**
 * Inspired by Confluent AvroConverter
 * @see io.confluent.connect.avro.AvroConverter
 */
class AvroSerializer(configs: Map<String, String>) : RocksDbRecordSerializer<SinkRecord> {
    companion object {
        const val MAGIC_BYTE: Byte = 0x0
        const val AVRO_CONFIG_PREFIX = "dedupe.avro"

        private fun extractConfigs(configs: Map<String, String>) =
            configs.filterKeys { it.startsWith(AVRO_CONFIG_PREFIX) }
                .mapKeys { it.key.substring(AVRO_CONFIG_PREFIX.length) }
    }

    private val avroConfigs = extractConfigs(configs)
    private val serializer = Serializer(avroConfigs)
    private val avroData = AvroData(AvroDataConfig(avroConfigs))

    override fun serialize(record: SinkRecord): ByteArray {
        Preconditions.checkArgument(record.key() != null, "Record key must not be null")

        val topic = record.topic()
        val schema = record.keySchema()
        val value = record.key()

        return try {
            val avroSchema: Schema = avroData.fromConnectSchema(schema)
            serializer.serialize(
                avroData.fromConnectData(schema, value),
                AvroSchema(avroSchema)
            )
        } catch (e: SerializationException) {
            throw DataException(
                String.format("Failed to serialize Avro data from topic %s :", topic),
                e
            )
        } catch (e: InvalidConfigurationException) {
            throw ConfigException(String.format("Failed to access Avro data from topic %s : %s", topic, e.message))
        }
    }

    private class Serializer(configs: Map<String, String>) {
        private val encoderFactory = EncoderFactory.get()
        private val datumWriterCache: MutableMap<Schema, DatumWriter<Any>> = ConcurrentHashMap()
        private val useSchemaReflection = configs[SCHEMA_REFLECTION_CONFIG]?.toBoolean() ?: false
        private val avroUseLogicalTypeConverters =
            configs[AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG]?.toBoolean() ?: false

        fun serialize(
            objValue: Any, schema: AvroSchema
        ): ByteArray {
            return try {
                val out = ByteArrayOutputStream()
                out.write(MAGIC_BYTE.toInt())
                // Not write schema id
                // out.write(ByteBuffer.allocate(idSize).putInt(id).array())
                val value: Any = if (objValue is NonRecordContainer) objValue.value else objValue
                val rawSchema = schema.rawSchema()
                if (rawSchema.type == Schema.Type.BYTES) {
                    when (value) {
                        is ByteArray -> out.write(value)
                        is ByteBuffer -> out.write(value.array())
                        else -> throw SerializationException(
                            "Unrecognized bytes object of type: " + value.javaClass.name
                        )
                    }
                } else {
                    writeDatum(out, value, rawSchema)
                }
                val bytes = out.toByteArray()
                out.close()
                bytes
            } catch (e: IOException) {
                throw SerializationException("Error serializing Avro message", e)
            } catch (e: RuntimeException) {
                throw SerializationException("Error serializing Avro message", e)
            }
        }

        @Suppress("UNCHECKED_CAST")
        private fun writeDatum(out: ByteArrayOutputStream, value: Any, rawSchema: Schema) {
            val encoder: BinaryEncoder = encoderFactory.directBinaryEncoder(out, null)
            val writer: DatumWriter<Any> = datumWriterCache.computeIfAbsent(rawSchema) {
                getDatumWriter(value, it) as DatumWriter<Any>
            }
            writer.write(value, encoder)
            encoder.flush()
        }

        private fun getDatumWriter(value: Any, schema: Schema): DatumWriter<*> {
            return if (value is SpecificRecord) {
                SpecificDatumWriter<Any>(schema)
            } else if (useSchemaReflection) {
                ReflectDatumWriter(schema)
            } else {
                val genericData = GenericData()
                if (avroUseLogicalTypeConverters) {
                    addLogicalTypeConversion(genericData)
                }
                GenericDatumWriter<Any>(schema, genericData)
            }
        }
    }
}
