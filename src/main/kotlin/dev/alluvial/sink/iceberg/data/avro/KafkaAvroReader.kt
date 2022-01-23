package dev.alluvial.sink.iceberg.data.avro

import dev.alluvial.sink.iceberg.data.KafkaSchemaUtil
import org.apache.avro.LogicalTypes
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.iceberg.avro.*
import org.apache.iceberg.data.avro.DecoderResolver
import org.apache.iceberg.types.Types.*
import java.util.concurrent.TimeUnit.*
import java.util.function.Supplier
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Type as AvroType
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

/**
 * @see org.apache.iceberg.avro.GenericAvroReader
 * @see org.apache.iceberg.flink.data.FlinkAvroReader
 * @see org.apache.iceberg.spark.data.SparkAvroReader
 */
class KafkaAvroReader(
    private val kafkaSchema: KafkaSchema,
    private val readSchema: AvroSchema,
) : DatumReader<KafkaStruct>, SupportsRowPosition {
    private val reader: ValueReader<KafkaStruct>
    private lateinit var fileSchema: AvroSchema

    init {
        @Suppress("UNCHECKED_CAST")
        this.reader = AvroWithPartnerByStructureVisitor.visit(kafkaSchema, readSchema, ReadBuilder())
            as ValueReader<KafkaStruct>
    }

    constructor(expectedSchema: IcebergSchema, readSchema: AvroSchema) :
        this(KafkaSchemaUtil.toKafkaSchema(expectedSchema), readSchema)

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
            valueReader: KafkaSchema?,
            primitive: AvroSchema
        ): ValueReader<*> {
            val logicalType = primitive.logicalType
            if (logicalType != null) {
                /** @see org.apache.avro.LogicalTypes */
                return when (logicalType.name) {
                    // LogicalTypes.DECIMAL
                    "decimal" -> {
                        val decimal = logicalType as LogicalTypes.Decimal
                        KafkaValueReaders.decimal(
                            ValueReaders.decimalBytesReader(primitive),
                            decimal.precision, decimal.scale
                        )
                    }
                    // LogicalTypes.UUID
                    "uuid" -> ValueReaders.uuids()
                    // LogicalTypes.DATE
                    "date" -> KafkaValueReaders.date()
                    // LogicalTypes.TIME_MILLIS
                    "time-millis" -> KafkaValueReaders.time(MILLISECONDS)
                    // LogicalTypes.TIME_MICROS
                    "time-micros" -> KafkaValueReaders.time(MICROSECONDS)
                    // LogicalTypes.TIMESTAMP_MILLIS
                    "timestamp-millis" -> KafkaValueReaders.timestamp(MILLISECONDS)
                    // LogicalTypes.TIMESTAMP_MICROS
                    "timestamp-micros" -> KafkaValueReaders.timestamp(MICROSECONDS)
                    // LogicalTypes.LOCAL_TIMESTAMP_MILLIS
                    "local-timestamp-millis" -> KafkaValueReaders.timestamp(MILLISECONDS)
                    // LogicalTypes.LOCAL_TIMESTAMP_MICROS
                    "local-timestamp-micros" -> KafkaValueReaders.timestamp(MICROSECONDS)
                    else -> throw IllegalArgumentException("Unsupported logical type: $logicalType")
                }
            }

            return when (primitive.type) {
                AvroType.NULL -> ValueReaders.nulls()
                AvroType.BOOLEAN -> ValueReaders.booleans()
                AvroType.INT -> ValueReaders.ints()
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
