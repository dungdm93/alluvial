package dev.alluvial.sink.iceberg.data.parquet

import dev.alluvial.sink.iceberg.data.toKafkaSchema
import dev.alluvial.utils.TimePrecision.*
import dev.alluvial.utils.timePrecision
import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.schema.Type
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Schema.Type as KafkaType

object KafkaParquetReader {
    fun buildReader(expectedSchema: Schema, fileSchema: MessageType): ParquetValueReader<*> {
        val kafkaSchema = expectedSchema.toKafkaSchema()
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

            val reader = kafkaLogicalType(sPrimitive, primitive, desc)
            if (reader != null) return reader

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

        private fun kafkaLogicalType(
            sType: KafkaSchema,
            type: PrimitiveType,
            desc: ColumnDescriptor
        ): ParquetValueReader<*>? {
            val logicalType = type.logicalTypeAnnotation

            @Suppress("RemoveRedundantQualifierName")
            return when (sType.name()) {
                /////////////// Debezium Logical Types ///////////////
                io.debezium.time.Date.SCHEMA_NAME ->
                    KafkaParquetReaders.unboxed<Int>(desc)
                io.debezium.time.Time.SCHEMA_NAME ->
                    KafkaParquetReaders.timeAsInt(logicalType.timePrecision(), MILLIS, desc)
                io.debezium.time.MicroTime.SCHEMA_NAME ->
                    KafkaParquetReaders.timeAsLong(logicalType.timePrecision(), MICROS, desc)
                io.debezium.time.NanoTime.SCHEMA_NAME ->
                    KafkaParquetReaders.timeAsLong(logicalType.timePrecision(), NANOS, desc)
                io.debezium.time.ZonedTime.SCHEMA_NAME ->
                    KafkaParquetReaders.zonedTimeAsString(logicalType.timePrecision(), desc)
                io.debezium.time.Timestamp.SCHEMA_NAME ->
                    KafkaParquetReaders.timestampAsLong(logicalType.timePrecision(), MILLIS, desc)
                io.debezium.time.MicroTimestamp.SCHEMA_NAME ->
                    KafkaParquetReaders.timestampAsLong(logicalType.timePrecision(), MICROS, desc)
                io.debezium.time.NanoTimestamp.SCHEMA_NAME ->
                    KafkaParquetReaders.timestampAsLong(logicalType.timePrecision(), NANOS, desc)
                io.debezium.time.ZonedTimestamp.SCHEMA_NAME ->
                    KafkaParquetReaders.zonedTimestampAsString(logicalType.timePrecision(), desc)
                io.debezium.time.Year.SCHEMA_NAME ->
                    KafkaParquetReaders.unboxed<Int>(desc)
                io.debezium.data.Enum.LOGICAL_NAME ->
                    KafkaParquetReaders.strings(desc)
                io.debezium.data.EnumSet.LOGICAL_NAME -> TODO()

                /////////////// Kafka Logical Types ///////////////
                org.apache.kafka.connect.data.Decimal.LOGICAL_NAME -> {
                    val decimal = logicalType as DecimalLogicalTypeAnnotation
                    when (type.primitiveTypeName) {
                        INT32 -> ParquetValueReaders.IntegerAsDecimalReader(desc, decimal.scale)
                        INT64 -> ParquetValueReaders.LongAsDecimalReader(desc, decimal.scale)
                        BINARY,
                        FIXED_LEN_BYTE_ARRAY -> ParquetValueReaders.BinaryAsDecimalReader(desc, decimal.scale)
                        else -> throw UnsupportedOperationException(
                            "Unsupported base type for decimal: ${type.primitiveTypeName}"
                        )
                    }
                }
                org.apache.kafka.connect.data.Date.LOGICAL_NAME ->
                    KafkaParquetReaders.date(desc)
                org.apache.kafka.connect.data.Time.LOGICAL_NAME ->
                    KafkaParquetReaders.timeAsDate(logicalType.timePrecision(), desc)
                org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME ->
                    KafkaParquetReaders.timestampAsDate(logicalType.timePrecision(), desc)
                else -> null
            }
        }
    }
}
