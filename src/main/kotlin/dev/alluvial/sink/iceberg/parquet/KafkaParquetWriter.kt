package dev.alluvial.sink.iceberg.parquet

import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaType
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import dev.alluvial.sink.iceberg.type.logical.logicalTypeConverter
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.schema.Type

@Suppress("INACCESSIBLE_TYPE")
object KafkaParquetWriter {
    fun buildWriter(sSchema: KafkaSchema, fileSchema: MessageType): ParquetValueWriter<*> {
        return WriteBuilder(fileSchema).visit(sSchema, fileSchema)
    }

    private class WriteBuilder(
        val message: MessageType
    ) : ParquetWithKafkaSchemaVisitor<ParquetValueWriter<*>>() {
        private fun newOption(fieldType: Type, writer: ParquetValueWriter<*>?): ParquetValueWriter<*>? {
            val maxD = message.getMaxDefinitionLevel(*path(fieldType.name))
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

            val repeatedD = message.getMaxDefinitionLevel(*repeatedPath)
            val repeatedR = message.getMaxRepetitionLevel(*repeatedPath)

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

            val repeatedD = message.getMaxDefinitionLevel(*repeatedPath)
            val repeatedR = message.getMaxRepetitionLevel(*repeatedPath)

            val keyWriter = newOption(repeated.getType(0), key)
            val valueWriter = newOption(repeated.getType(1), value)
            return ParquetValueWriters.maps(repeatedD, repeatedR, keyWriter, valueWriter)
        }

        override fun primitive(
            sPrimitive: KafkaSchema,
            primitive: PrimitiveType
        ): ParquetValueWriter<*> {
            val desc = message.getColumnDescription(currentPath())

            val converter = sPrimitive.logicalTypeConverter()
            if (converter != null) {
                val ctx = ParquetWriterContext.primitive(desc)
                return converter.parquetWriter(sPrimitive, primitive, ctx)
            }

            /** @link https://github.com/apache/parquet-format/blob/master/LogicalTypes.md */
            return when (primitive.primitiveTypeName) {
                BOOLEAN -> ParquetValueWriters.booleans(desc)
                INT32 -> return when (sPrimitive.type()) {
                    KafkaType.INT8 -> ParquetValueWriters.tinyints(desc)
                    KafkaType.INT16 -> ParquetValueWriters.shorts(desc)
                    else -> ParquetValueWriters.ints(desc)
                }
                INT64 -> ParquetValueWriters.longs(desc)
                INT96 -> TODO()
                FLOAT -> ParquetValueWriters.floats(desc)
                DOUBLE -> ParquetValueWriters.doubles(desc)
                BINARY -> when (sPrimitive.type()) {
                    KafkaType.STRING -> ParquetValueWriters.strings(desc)
                    else -> KafkaParquetWriters.bytes(desc)
                }
                FIXED_LEN_BYTE_ARRAY -> KafkaParquetWriters.bytes(desc)
                else -> throw IllegalArgumentException("Unknown primitiveType=$primitive with logicalType=${primitive.logicalTypeAnnotation}")
            }
        }
    }
}
