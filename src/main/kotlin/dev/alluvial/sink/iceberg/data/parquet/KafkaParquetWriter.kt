package dev.alluvial.sink.iceberg.data.parquet

import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation.*
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.schema.Type
import org.apache.kafka.connect.data.Schema as KafkaSchema

@Suppress("INACCESSIBLE_TYPE")
object KafkaParquetWriter {
    fun buildWriter(schema: KafkaSchema, type: MessageType): ParquetValueWriter<*> {
        return WriteBuilder(type).visit(schema, type)
    }

    private class WriteBuilder(val type: MessageType) : ParquetWithKafkaSchemaVisitor<ParquetValueWriter<*>>() {
        private fun newOption(fieldType: Type, writer: ParquetValueWriter<*>?): ParquetValueWriter<*>? {
            val maxD = type.getMaxDefinitionLevel(*path(fieldType.name))
            return ParquetValueWriters.option(fieldType, maxD, writer)
        }

        override fun struct(
            sStruct: KafkaSchema,
            struct: GroupType,
            fields: List<ParquetValueWriter<*>?>
        ): ParquetValueWriter<*> {
            val fieldWriters = fields.mapIndexed { idx, field ->
                newOption(struct.getType(idx), field)
            }
            return KafkaParquetWriters.struct(fieldWriters, sStruct)
        }

        override fun list(
            sArray: KafkaSchema,
            array: GroupType,
            element: ParquetValueWriter<*>?
        ): ParquetValueWriter<*> {
            val repeated = array.getType(0).asGroupType()
            val repeatedPath = currentPath()

            val repeatedD = type.getMaxDefinitionLevel(*repeatedPath)
            val repeatedR = type.getMaxRepetitionLevel(*repeatedPath)

            val elementWriter = newOption(repeated.getType(0), element)
            return ParquetValueWriters.collections(repeatedD, repeatedR, elementWriter)
        }

        override fun map(
            sMap: KafkaSchema,
            map: GroupType,
            key: ParquetValueWriter<*>?,
            value: ParquetValueWriter<*>?
        ): ParquetValueWriter<*> {
            val repeated = map.getType(0).asGroupType()
            val repeatedPath = currentPath()

            val repeatedD = type.getMaxDefinitionLevel(*repeatedPath)
            val repeatedR = type.getMaxRepetitionLevel(*repeatedPath)

            val keyWriter = newOption(repeated.getType(0), key)
            val valueWriter = newOption(repeated.getType(1), value)
            return ParquetValueWriters.maps(repeatedD, repeatedR, keyWriter, valueWriter)
        }

        override fun primitive(sPrimitive: KafkaSchema, primitive: PrimitiveType): ParquetValueWriter<*> {
            val desc = type.getColumnDescription(currentPath())

            /** @link https://github.com/apache/parquet-format/blob/master/LogicalTypes.md */
            return when (val logicalType = primitive.logicalTypeAnnotation) {
                // String Types
                is StringLogicalTypeAnnotation,
                is EnumLogicalTypeAnnotation,
                is UUIDLogicalTypeAnnotation -> ParquetValueWriters.strings(desc)
                // Numeric Types
                is IntLogicalTypeAnnotation -> when (logicalType.bitWidth) {
                    8 -> ParquetValueWriters.tinyints(desc)
                    16 -> ParquetValueWriters.shorts(desc)
                    32 -> ParquetValueWriters.ints(desc)
                    64 -> ParquetValueWriters.longs(desc)
                    else -> throw UnsupportedOperationException(
                        "Unsupported bitWidth for integer: ${logicalType.bitWidth}"
                    )
                }
                is DecimalLogicalTypeAnnotation -> when (primitive.primitiveTypeName) {
                    INT32 -> ParquetValueWriters.decimalAsInteger(desc, logicalType.precision, logicalType.scale)
                    INT64 -> ParquetValueWriters.decimalAsLong(desc, logicalType.precision, logicalType.scale)
                    BINARY,
                    FIXED_LEN_BYTE_ARRAY -> ParquetValueWriters.decimalAsFixed(
                        desc, logicalType.precision, logicalType.scale
                    )
                    else -> throw UnsupportedOperationException(
                        "Unsupported base type for decimal: ${primitive.primitiveTypeName}"
                    )
                }
                // Temporal Types
                is DateLogicalTypeAnnotation -> KafkaParquetWriters.date(desc)
                is TimeLogicalTypeAnnotation -> KafkaParquetWriters.time(desc, logicalType)
                is TimestampLogicalTypeAnnotation -> KafkaParquetWriters.timestamp(desc, logicalType)
                is IntervalLogicalTypeAnnotation -> TODO()
                // Nested Types
                is JsonLogicalTypeAnnotation -> ParquetValueWriters.strings(desc)
                is BsonLogicalTypeAnnotation -> KafkaParquetWriters.bytes(desc)
                // logicalType = null
                null -> when (primitive.primitiveTypeName) {
                    BOOLEAN -> ParquetValueWriters.booleans(desc)
                    INT32 -> ParquetValueWriters.ints(desc)
                    INT64 -> ParquetValueWriters.longs(desc)
                    INT96 -> TODO()
                    FLOAT -> ParquetValueWriters.floats(desc)
                    DOUBLE -> ParquetValueWriters.doubles(desc)
                    BINARY -> KafkaParquetWriters.bytes(desc)
                    FIXED_LEN_BYTE_ARRAY -> KafkaParquetWriters.bytes(desc)
                    else -> TODO()
                }
                else -> throw IllegalArgumentException("Unknown primitiveType=$primitive with logicalType=$logicalType")
            }
        }
    }
}
