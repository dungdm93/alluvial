package dev.alluvial.sink.iceberg.avro

import dev.alluvial.sink.iceberg.type.logical.logicalTypeConverter
import dev.alluvial.utils.TimePrecision.*
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.iceberg.avro.AvroWithPartnerByStructureVisitor
import org.apache.iceberg.avro.SupportsRowPosition
import org.apache.iceberg.avro.ValueReader
import org.apache.iceberg.avro.ValueReaders
import org.apache.iceberg.data.avro.DecoderResolver
import java.util.function.Supplier
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Type as AvroType
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Schema.Type as KafkaType
import org.apache.kafka.connect.data.Struct as KafkaStruct

/**
 * @see org.apache.iceberg.avro.GenericAvroReader
 * @see org.apache.iceberg.flink.data.FlinkAvroReader
 * @see org.apache.iceberg.spark.data.SparkAvroReader
 */
class KafkaAvroReader(
    kafkaSchema: KafkaSchema,
    private val readSchema: AvroSchema,
) : DatumReader<KafkaStruct>, SupportsRowPosition {
    private val reader: ValueReader<KafkaStruct>
    private lateinit var fileSchema: AvroSchema

    init {
        @Suppress("UNCHECKED_CAST")
        this.reader = AvroWithPartnerByStructureVisitor.visit(kafkaSchema, readSchema, ReadBuilder())
            as ValueReader<KafkaStruct>
    }

    override fun setSchema(schema: AvroSchema) {
        this.fileSchema = AvroSchema.applyAliases(schema, readSchema)
    }

    override fun read(reuse: KafkaStruct?, decoder: Decoder): KafkaStruct {
        return DecoderResolver.resolveAndRead(decoder, readSchema, fileSchema, reader, reuse)
    }

    override fun setRowPositionSupplier(posSupplier: Supplier<Long>) {
        if (reader is SupportsRowPosition) {
            (reader as SupportsRowPosition).setRowPositionSupplier(posSupplier)
        }
    }

    private class ReadBuilder : AvroWithKafkaSchemaVisitor<ValueReader<*>>() {
        override fun record(
            expected: KafkaSchema,
            record: AvroSchema,
            names: List<String>,
            fieldReaders: List<ValueReader<*>>
        ): ValueReader<*> {
            return KafkaValueReaders.struct(names, fieldReaders, expected)
        }

        override fun union(
            expected: KafkaSchema,
            union: AvroSchema,
            options: List<ValueReader<*>>
        ): ValueReader<*> {
            return ValueReaders.union(options)
        }

        override fun array(
            expected: KafkaSchema,
            array: AvroSchema,
            elementReader: ValueReader<*>
        ): ValueReader<*> {
            return KafkaValueReaders.array(elementReader)
        }

        override fun map(
            expected: KafkaSchema,
            map: AvroSchema,
            valueReader: ValueReader<*>
        ): ValueReader<*> {
            return KafkaValueReaders.map(ValueReaders.strings(), valueReader)
        }

        override fun map(
            expected: KafkaSchema,
            map: AvroSchema,
            keyReader: ValueReader<*>,
            valueReader: ValueReader<*>
        ): ValueReader<*> {
            return KafkaValueReaders.arrayMap(keyReader, valueReader)
        }

        override fun primitive(
            type: KafkaSchema?,
            primitive: AvroSchema
        ): ValueReader<*> {
            val converter = type?.logicalTypeConverter()
            if (converter != null) {
                return converter.avroReader(type, primitive)
            }

            return when (primitive.type) {
                AvroType.NULL -> ValueReaders.nulls()
                AvroType.BOOLEAN -> ValueReaders.booleans()
                AvroType.INT -> when (type?.type()) {
                    KafkaType.INT8 -> KafkaValueReaders.bytes(ValueReaders.ints())
                    KafkaType.INT16 -> KafkaValueReaders.shorts(ValueReaders.ints())
                    KafkaType.INT32 -> ValueReaders.ints()
                    else -> throw IllegalArgumentException("Unsupported read avro INT to kafka ${type?.type()}")
                }
                AvroType.LONG -> ValueReaders.longs()
                AvroType.FLOAT -> ValueReaders.floats()
                AvroType.DOUBLE -> ValueReaders.doubles()
                AvroType.STRING -> ValueReaders.strings()
                AvroType.FIXED -> ValueReaders.fixed(primitive.fixedSize)
                AvroType.BYTES -> ValueReaders.bytes()
                AvroType.ENUM -> ValueReaders.enums(primitive.enumSymbols)
                else -> throw IllegalArgumentException("Unsupported type: $primitive")
            }
        }
    }
}
