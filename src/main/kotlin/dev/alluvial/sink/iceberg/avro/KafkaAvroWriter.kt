package dev.alluvial.sink.iceberg.avro

import dev.alluvial.sink.iceberg.type.AvroSchema
import dev.alluvial.sink.iceberg.type.AvroType
import dev.alluvial.sink.iceberg.type.AvroValueWriter
import dev.alluvial.sink.iceberg.type.AvroValueWriters
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.KafkaType
import dev.alluvial.sink.iceberg.type.logical.logicalTypeConverter
import dev.alluvial.utils.TimePrecision.*
import org.apache.avro.io.Encoder
import org.apache.iceberg.FieldMetrics
import org.apache.iceberg.avro.AvroWithPartnerByStructureVisitor
import org.apache.iceberg.avro.MetricsAwareDatumWriter
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import java.util.stream.Stream

/**
 * @see org.apache.iceberg.avro.GenericAvroWriter
 * @see org.apache.iceberg.flink.data.FlinkAvroWriter
 * @see org.apache.iceberg.spark.data.SparkAvroWriter
 */
class KafkaAvroWriter(
    private val sSchema: KafkaSchema
) : MetricsAwareDatumWriter<KafkaStruct> {
    private lateinit var writer: AvroValueWriter<KafkaStruct>

    override fun setSchema(avroSchema: AvroSchema) {
        @Suppress("UNCHECKED_CAST")
        writer = AvroWithPartnerByStructureVisitor.visit(sSchema, avroSchema, WriteBuilder())
            as AvroValueWriter<KafkaStruct>
    }

    override fun write(datum: KafkaStruct, out: Encoder) {
        writer.write(datum, out)
    }

    override fun metrics(): Stream<FieldMetrics<*>> {
        return writer.metrics()
    }

    private class WriteBuilder : AvroWithKafkaSchemaVisitor<AvroValueWriter<*>>() {
        override fun record(
            struct: KafkaSchema,
            record: AvroSchema,
            names: List<String>,
            fields: List<AvroValueWriter<*>>
        ): AvroValueWriter<*> {
            return KafkaAvroWriters.struct(fields, names)
        }

        override fun union(
            type: KafkaSchema,
            union: AvroSchema,
            options: List<AvroValueWriter<*>>
        ): AvroValueWriter<*> {
            Preconditions.checkArgument(
                options.contains(AvroValueWriters.nulls()),
                "Cannot create writer for non-option union: %s", union
            )
            Preconditions.checkArgument(
                options.size == 2,
                "Cannot create writer for non-option union: %s", union
            )
            return if (union.types[0].type == AvroType.NULL) {
                AvroValueWriters.option(0, options[1])
            } else {
                AvroValueWriters.option(1, options[0])
            }
        }

        override fun array(
            sArray: KafkaSchema,
            array: AvroSchema,
            element: AvroValueWriter<*>
        ): AvroValueWriter<*> {
            return KafkaAvroWriters.array(element)
        }

        override fun map(
            sMap: KafkaSchema,
            map: AvroSchema,
            value: AvroValueWriter<*>
        ): AvroValueWriter<*> {
            return KafkaAvroWriters.map(AvroValueWriters.strings(), value)
        }

        override fun map(
            sMap: KafkaSchema,
            map: AvroSchema,
            key: AvroValueWriter<*>,
            value: AvroValueWriter<*>
        ): AvroValueWriter<*> {
            return KafkaAvroWriters.arrayMap(key, value)
        }

        override fun primitive(
            type: KafkaSchema?,
            primitive: AvroSchema
        ): AvroValueWriter<*> {
            val converter = type?.logicalTypeConverter()
            if (converter != null) {
                return converter.avroWriter(type, primitive)
            }

            return when (primitive.type) {
                AvroType.NULL -> AvroValueWriters.nulls()
                AvroType.BOOLEAN -> AvroValueWriters.booleans()
                AvroType.INT -> when (type?.type()) {
                    KafkaType.INT8 -> AvroValueWriters.tinyints()
                    KafkaType.INT16 -> AvroValueWriters.shorts()
                    else -> AvroValueWriters.ints() // KafkaType.INT32
                }
                AvroType.LONG -> AvroValueWriters.longs()
                AvroType.FLOAT -> AvroValueWriters.floats()
                AvroType.DOUBLE -> AvroValueWriters.doubles()
                AvroType.STRING -> AvroValueWriters.strings()
                AvroType.FIXED -> AvroValueWriters.fixed(primitive.fixedSize)
                AvroType.BYTES -> KafkaAvroWriters.bytes()
                else -> throw IllegalArgumentException("Unsupported type: $primitive")
            }
        }
    }
}
