package dev.alluvial.sink.iceberg.data.avro

import org.apache.avro.LogicalTypes
import org.apache.avro.io.Encoder
import org.apache.iceberg.FieldMetrics
import org.apache.iceberg.avro.AvroWithPartnerByStructureVisitor
import org.apache.iceberg.avro.MetricsAwareDatumWriter
import org.apache.iceberg.avro.ValueWriter
import org.apache.iceberg.avro.ValueWriters
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import java.util.concurrent.TimeUnit
import java.util.stream.Stream
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Type as AvroType
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Schema.Type as KafkaType
import org.apache.kafka.connect.data.Struct as KafkaStruct

/**
 * @see org.apache.iceberg.avro.GenericAvroWriter
 * @see org.apache.iceberg.flink.data.FlinkAvroWriter
 * @see org.apache.iceberg.spark.data.SparkAvroWriter
 */
class KafkaAvroWriter(private val kafkaSchema: KafkaSchema) : MetricsAwareDatumWriter<KafkaStruct> {
    private lateinit var writer: ValueWriter<KafkaStruct>

    override fun setSchema(avroSchema: AvroSchema) {
        @Suppress("UNCHECKED_CAST")
        writer = AvroWithPartnerByStructureVisitor.visit(kafkaSchema, avroSchema, WriteBuilder())
            as ValueWriter<KafkaStruct>
    }

    override fun write(datum: KafkaStruct, out: Encoder) {
        writer.write(datum, out)
    }

    override fun metrics(): Stream<FieldMetrics<*>> {
        return writer.metrics()
    }

    private class WriteBuilder : AvroWithKafkaSchemaVisitor<ValueWriter<*>>() {
        override fun record(
            struct: KafkaSchema,
            record: AvroSchema,
            names: List<String>,
            fields: List<ValueWriter<*>>
        ): ValueWriter<*> {
            return KafkaValueWriters.struct(fields, names)
        }

        override fun union(
            type: KafkaSchema,
            union: AvroSchema,
            options: List<ValueWriter<*>>
        ): ValueWriter<*> {
            Preconditions.checkArgument(
                options.contains(ValueWriters.nulls()),
                "Cannot create writer for non-option union: %s", union
            )
            Preconditions.checkArgument(
                options.size == 2,
                "Cannot create writer for non-option union: %s", union
            )
            return if (union.types[0].type == AvroType.NULL) {
                ValueWriters.option(0, options[1])
            } else {
                ValueWriters.option(1, options[0])
            }
        }

        override fun array(
            sArray: KafkaSchema,
            array: AvroSchema,
            element: ValueWriter<*>
        ): ValueWriter<*> {
            return KafkaValueWriters.array(element)
        }

        override fun map(
            sMap: KafkaSchema,
            map: AvroSchema,
            value: ValueWriter<*>
        ): ValueWriter<*> {
            return KafkaValueWriters.map(ValueWriters.strings(), value)
        }

        override fun map(
            sMap: KafkaSchema,
            map: AvroSchema,
            key: ValueWriter<*>,
            value: ValueWriter<*>
        ): ValueWriter<*> {
            return KafkaValueWriters.arrayMap(key, value)
        }

        override fun primitive(
            type: KafkaSchema?,
            primitive: AvroSchema
        ): ValueWriter<*> {
            val logicalType = primitive.logicalType
            if (logicalType != null) {
                /** @see org.apache.avro.LogicalTypes */
                return when (logicalType.name) {
                    // LogicalTypes.DECIMAL
                    "decimal" -> {
                        val decimal = logicalType as LogicalTypes.Decimal
                        ValueWriters.decimal(decimal.precision, decimal.scale)
                    }
                    // LogicalTypes.UUID
                    "uuid" -> ValueWriters.uuids()
                    // LogicalTypes.DATE
                    "date" -> KafkaValueWriters.date()
                    // LogicalTypes.TIME_MILLIS
                    "time-millis" -> KafkaValueWriters.time(TimeUnit.MILLISECONDS)
                    // LogicalTypes.TIME_MICROS
                    "time-micros" -> KafkaValueWriters.time(TimeUnit.MICROSECONDS)
                    // LogicalTypes.TIMESTAMP_MILLIS
                    "timestamp-millis" -> KafkaValueWriters.timestamp(TimeUnit.MILLISECONDS)
                    // LogicalTypes.TIMESTAMP_MICROS
                    "timestamp-micros" -> KafkaValueWriters.timestamp(TimeUnit.MICROSECONDS)
                    // LogicalTypes.LOCAL_TIMESTAMP_MILLIS
                    "local-timestamp-millis" -> KafkaValueWriters.timestamp(TimeUnit.MILLISECONDS)
                    // LogicalTypes.LOCAL_TIMESTAMP_MICROS
                    "local-timestamp-micros" -> KafkaValueWriters.timestamp(TimeUnit.MICROSECONDS)
                    else -> throw IllegalArgumentException("Unsupported logical type: $logicalType")
                }
            }

            return when (primitive.type) {
                AvroType.NULL -> ValueWriters.nulls()
                AvroType.BOOLEAN -> ValueWriters.booleans()
                AvroType.INT -> when (type?.type()) {
                    KafkaType.INT8 -> ValueWriters.tinyints()
                    KafkaType.INT16 -> ValueWriters.shorts()
                    else -> ValueWriters.ints() // KafkaType.INT32
                }
                AvroType.LONG -> ValueWriters.longs()
                AvroType.FLOAT -> ValueWriters.floats()
                AvroType.DOUBLE -> ValueWriters.doubles()
                AvroType.STRING -> ValueWriters.strings()
                AvroType.FIXED -> ValueWriters.fixed(primitive.fixedSize)
                AvroType.BYTES -> KafkaValueWriters.bytes()
                else -> throw IllegalArgumentException("Unsupported type: $primitive")
            }
        }
    }
}