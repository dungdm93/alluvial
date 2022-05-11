package dev.alluvial.sink.iceberg.parquet

import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaType
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.logicalTypeConverter
import dev.alluvial.sink.iceberg.type.toKafkaSchema
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.schema.Type

object KafkaParquetReader {
    fun buildReader(iSchema: IcebergSchema, fileSchema: MessageType): ParquetValueReader<*> {
        val kafkaSchema = iSchema.toKafkaSchema()
        return buildReader(kafkaSchema, fileSchema)
    }

    fun buildReader(sSchema: KafkaSchema, fileSchema: MessageType): ParquetValueReader<*> {
        return ReadBuilder(fileSchema).visit(sSchema, fileSchema)
    }

    private class ReadBuilder(
        private val message: MessageType
    ) : ParquetWithKafkaSchemaVisitor<ParquetValueReader<*>>() {
        private fun newOption(fieldType: Type, reader: ParquetValueReader<*>?): ParquetValueReader<*> {
            val maxD = message.getMaxDefinitionLevel(*path(fieldType.name)) - 1
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

            val repeatedD: Int = message.getMaxDefinitionLevel(*repeatedPath) - 1
            val repeatedR: Int = message.getMaxRepetitionLevel(*repeatedPath) - 1

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

            val repeatedD = message.getMaxDefinitionLevel(*repeatedPath) - 1
            val repeatedR = message.getMaxRepetitionLevel(*repeatedPath) - 1

            val keyWriter = newOption(repeated.getType(0), key)
            val valueWriter = newOption(repeated.getType(1), value)
            return KafkaParquetReaders.map(repeatedD, repeatedR, keyWriter, valueWriter)
        }

        override fun primitive(
            sPrimitive: KafkaSchema,
            primitive: PrimitiveType
        ): ParquetValueReader<*> {
            val desc = message.getColumnDescription(currentPath())

            val converter = sPrimitive.logicalTypeConverter()
            if (converter != null) {
                val ctx = ParquetReaderContext.primitive(desc)
                return converter.parquetReader(sPrimitive, primitive, ctx)
            }

            /** @link https://github.com/apache/parquet-format/blob/master/LogicalTypes.md */
            return when (primitive.primitiveTypeName) {
                BOOLEAN -> KafkaParquetReaders.unboxed<Boolean>(desc)
                INT32 -> when (sPrimitive.type()) {
                    KafkaType.INT8 -> KafkaParquetReaders.tinyints(desc)
                    KafkaType.INT16 -> KafkaParquetReaders.shorts(desc)
                    else -> KafkaParquetReaders.unboxed<Int>(desc)
                }
                INT64 -> KafkaParquetReaders.unboxed<Long>(desc)
                INT96 -> TODO()
                FLOAT -> KafkaParquetReaders.unboxed<Float>(desc)
                DOUBLE -> KafkaParquetReaders.unboxed<Double>(desc)
                BINARY -> when (sPrimitive.type()) {
                    KafkaType.STRING -> KafkaParquetReaders.strings(desc)
                    else -> KafkaParquetReaders.byteArrays(desc)
                }
                FIXED_LEN_BYTE_ARRAY -> KafkaParquetReaders.byteArrays(desc)
                else -> throw IllegalArgumentException("Unknown primitiveType=$primitive")
            }
        }
    }
}
