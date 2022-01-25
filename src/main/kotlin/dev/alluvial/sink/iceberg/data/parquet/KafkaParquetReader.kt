package dev.alluvial.sink.iceberg.data.parquet

import dev.alluvial.sink.iceberg.data.KafkaSchemaUtil
import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.parquet.schema.*
import org.apache.parquet.schema.LogicalTypeAnnotation.*
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Schema.Type as KafkaType

object KafkaParquetReader {
    fun buildReader(expectedSchema: Schema, fileSchema: MessageType): ParquetValueReader<*> {
        val kafkaSchema = KafkaSchemaUtil.toKafkaSchema(expectedSchema)
        return buildReader(kafkaSchema, fileSchema)
    }

    fun buildReader(sSchema: KafkaSchema, fileSchema: MessageType): ParquetValueReader<*> {
        return ReadBuilder(fileSchema).visit(sSchema, fileSchema)
    }

    private class ReadBuilder(private val type: MessageType) : ParquetWithKafkaSchemaVisitor<ParquetValueReader<*>>() {
        private fun newOption(fieldType: Type, reader: ParquetValueReader<*>?): ParquetValueReader<*> {
            val maxD = type.getMaxDefinitionLevel(*path(fieldType.name)) - 1
            return ParquetValueReaders.option(fieldType, maxD, reader)
        }

        override fun struct(
            sStruct: KafkaSchema,
            struct: GroupType,
            fields: List<ParquetValueReader<*>?>
        ): ParquetValueReader<*> {
            val fieldReaders = struct.fields.mapIndexed { idx, fieldType ->
                val fieldReader = fields[idx]
                newOption(fieldType, fieldReader)
            }
            return KafkaParquetReaders.struct(struct.fields, fieldReaders, sStruct)
        }

        override fun list(
            sArray: KafkaSchema,
            array: GroupType,
            element: ParquetValueReader<*>?,
        ): ParquetValueReader<*> {
            val repeated = array.getType(0).asGroupType()
            val repeatedPath = currentPath()

            val repeatedD: Int = type.getMaxDefinitionLevel(*repeatedPath) - 1
            val repeatedR: Int = type.getMaxRepetitionLevel(*repeatedPath) - 1

            val elementReader = newOption(repeated.getType(0), element)
            return KafkaParquetReaders.list(repeatedD, repeatedR, elementReader)
        }

        override fun map(
            sMap: KafkaSchema,
            map: GroupType,
            key: ParquetValueReader<*>?,
            value: ParquetValueReader<*>?,
        ): ParquetValueReader<*> {
            val repeated = map.getType(0).asGroupType()
            val repeatedPath = currentPath()

            val repeatedD = type.getMaxDefinitionLevel(*repeatedPath) - 1
            val repeatedR = type.getMaxRepetitionLevel(*repeatedPath) - 1

            val keyWriter = newOption(repeated.getType(0), key)
            val valueWriter = newOption(repeated.getType(1), value)
            return KafkaParquetReaders.map(repeatedD, repeatedR, keyWriter, valueWriter)
        }

        override fun primitive(
            sPrimitive: KafkaSchema,
            primitive: PrimitiveType
        ): ParquetValueReader<*> {
            val desc = type.getColumnDescription(currentPath())

            /** @link https://github.com/apache/parquet-format/blob/master/LogicalTypes.md */
            return when (val logicalType = primitive.logicalTypeAnnotation) {
                // String Types
                is StringLogicalTypeAnnotation,
                is EnumLogicalTypeAnnotation,
                is UUIDLogicalTypeAnnotation -> KafkaParquetReaders.strings(desc)
                // Numeric Types
                is IntLogicalTypeAnnotation -> when (logicalType.bitWidth) {
                    8,
                    16,
                    32 -> if (sPrimitive.type() == KafkaType.INT64)
                        ParquetValueReaders.IntAsLongReader(desc) else
                        KafkaParquetReaders.unboxed<Int>(desc)
                    64 -> KafkaParquetReaders.unboxed<Long>(desc)
                    else -> throw UnsupportedOperationException(
                        "Unsupported bitWidth for integer: ${logicalType.bitWidth}"
                    )
                }
                is DecimalLogicalTypeAnnotation -> when (primitive.primitiveTypeName) {
                    INT32 -> ParquetValueReaders.IntegerAsDecimalReader(desc, logicalType.scale)
                    INT64 -> ParquetValueReaders.LongAsDecimalReader(desc, logicalType.scale)
                    BINARY,
                    FIXED_LEN_BYTE_ARRAY -> ParquetValueReaders.BinaryAsDecimalReader(desc, logicalType.scale)
                    else -> throw UnsupportedOperationException(
                        "Unsupported base type for decimal: ${primitive.primitiveTypeName}"
                    )
                }
                // Temporal Types
                is DateLogicalTypeAnnotation -> KafkaParquetReaders.date(desc)
                is TimeLogicalTypeAnnotation -> KafkaParquetReaders.time(desc, logicalType)
                is TimestampLogicalTypeAnnotation -> KafkaParquetReaders.timestamp(desc, logicalType)
                is IntervalLogicalTypeAnnotation -> TODO()
                // Nested Types
                is JsonLogicalTypeAnnotation -> KafkaParquetReaders.strings(desc)
                is BsonLogicalTypeAnnotation -> KafkaParquetReaders.byteArrays(desc)
                // logicalType = null
                null -> when (primitive.primitiveTypeName) {
                    BOOLEAN -> KafkaParquetReaders.unboxed<Boolean>(desc)
                    INT32 -> if (sPrimitive.type() == KafkaType.INT64)
                        ParquetValueReaders.IntAsLongReader(desc) else
                        KafkaParquetReaders.unboxed<Int>(desc)
                    INT64 -> KafkaParquetReaders.unboxed<Long>(desc)
                    INT96 -> TODO()
                    FLOAT -> if (sPrimitive.type() == KafkaType.FLOAT64)
                        ParquetValueReaders.FloatAsDoubleReader(desc) else
                        KafkaParquetReaders.unboxed<Float>(desc)
                    DOUBLE -> KafkaParquetReaders.unboxed<Double>(desc)
                    BINARY,
                    FIXED_LEN_BYTE_ARRAY -> KafkaParquetReaders.byteArrays(desc)
                    else -> throw IllegalArgumentException("Unknown primitiveType=$primitive")
                }
                else -> throw IllegalArgumentException("Unknown primitiveType=$primitive with logicalType=$logicalType")
            }
        }
    }
}
