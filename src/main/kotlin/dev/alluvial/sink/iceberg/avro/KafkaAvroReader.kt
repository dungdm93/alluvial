package dev.alluvial.sink.iceberg.avro

import dev.alluvial.sink.iceberg.type.AvroSchema
import dev.alluvial.sink.iceberg.type.AvroType
import dev.alluvial.sink.iceberg.type.AvroValueReader
import dev.alluvial.sink.iceberg.type.AvroValueReaders
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.KafkaType
import dev.alluvial.sink.iceberg.type.logical.logicalTypeConverter
import dev.alluvial.utils.TimePrecision.*
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.iceberg.avro.AvroWithPartnerByStructureVisitor
import org.apache.iceberg.avro.SupportsRowPosition
import org.apache.iceberg.data.avro.DecoderResolver
import java.util.function.Supplier

/**
 * @see org.apache.iceberg.avro.GenericAvroReader
 * @see org.apache.iceberg.flink.data.FlinkAvroReader
 * @see org.apache.iceberg.spark.data.SparkAvroReader
 */
class KafkaAvroReader(
    private val sSchema: KafkaSchema,
    private val readSchema: AvroSchema,
) : DatumReader<KafkaStruct>, SupportsRowPosition {
    private val reader: AvroValueReader<KafkaStruct>
    private lateinit var fileSchema: AvroSchema

    init {
        @Suppress("UNCHECKED_CAST")
        this.reader = AvroWithPartnerByStructureVisitor.visit(sSchema, readSchema, ReadBuilder())
            as AvroValueReader<KafkaStruct>
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

    private class ReadBuilder : AvroWithKafkaSchemaVisitor<AvroValueReader<*>>() {
        override fun record(
            expected: KafkaSchema,
            record: AvroSchema,
            names: List<String>,
            fieldReaders: List<AvroValueReader<*>>
        ): AvroValueReader<*> {
            return KafkaAvroReaders.struct(names, fieldReaders, expected)
        }

        override fun union(
            expected: KafkaSchema,
            union: AvroSchema,
            options: List<AvroValueReader<*>>
        ): AvroValueReader<*> {
            return AvroValueReaders.union(options)
        }

        override fun array(
            expected: KafkaSchema,
            array: AvroSchema,
            elementReader: AvroValueReader<*>
        ): AvroValueReader<*> {
            return KafkaAvroReaders.array(elementReader)
        }

        override fun map(
            expected: KafkaSchema,
            map: AvroSchema,
            valueReader: AvroValueReader<*>
        ): AvroValueReader<*> {
            return KafkaAvroReaders.map(AvroValueReaders.strings(), valueReader)
        }

        override fun map(
            expected: KafkaSchema,
            map: AvroSchema,
            keyReader: AvroValueReader<*>,
            valueReader: AvroValueReader<*>
        ): AvroValueReader<*> {
            return KafkaAvroReaders.arrayMap(keyReader, valueReader)
        }

        override fun primitive(
            type: KafkaSchema?,
            primitive: AvroSchema
        ): AvroValueReader<*> {
            val converter = type?.logicalTypeConverter()
            if (converter != null) {
                return converter.avroReader(type, primitive)
            }

            return when (primitive.type) {
                AvroType.NULL -> AvroValueReaders.nulls()
                AvroType.BOOLEAN -> AvroValueReaders.booleans()
                AvroType.INT -> when (type?.type()) {
                    KafkaType.INT8 -> KafkaAvroReaders.bytes(AvroValueReaders.ints())
                    KafkaType.INT16 -> KafkaAvroReaders.shorts(AvroValueReaders.ints())
                    KafkaType.INT32 -> AvroValueReaders.ints()
                    else -> throw IllegalArgumentException("Unsupported read avro INT to kafka ${type?.type()}")
                }
                AvroType.LONG -> AvroValueReaders.longs()
                AvroType.FLOAT -> AvroValueReaders.floats()
                AvroType.DOUBLE -> AvroValueReaders.doubles()
                AvroType.STRING -> AvroValueReaders.strings()
                AvroType.FIXED -> AvroValueReaders.fixed(primitive.fixedSize)
                AvroType.BYTES -> AvroValueReaders.bytes()
                AvroType.ENUM -> AvroValueReaders.enums(primitive.enumSymbols)
                else -> throw IllegalArgumentException("Unsupported type: $primitive")
            }
        }
    }
}
