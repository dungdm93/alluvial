package dev.alluvial.sink.iceberg.data.parquet

import dev.alluvial.utils.TimePrecision.*
import dev.alluvial.utils.timePrecision
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.schema.Type
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Schema.Type as KafkaType

@Suppress("INACCESSIBLE_TYPE")
object KafkaParquetWriter {
    fun buildWriter(schema: KafkaSchema, type: MessageType): ParquetValueWriter<*> {
        return WriteBuilder(type).visit(schema, type)
    }

    private class WriteBuilder(val message: MessageType) : ParquetWithKafkaSchemaVisitor<ParquetValueWriter<*>>() {
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

            val writer = kafkaLogicalType(sPrimitive, primitive, desc)
            if (writer != null) return writer

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

        private fun kafkaLogicalType(
            sType: KafkaSchema,
            type: PrimitiveType,
            desc: ColumnDescriptor,
        ): ParquetValueWriter<*>? {
            val logicalType = type.logicalTypeAnnotation

            @Suppress("RemoveRedundantQualifierName")
            return when (sType.name()) {
                /////////////// Debezium Logical Types ///////////////
                io.debezium.time.Date.SCHEMA_NAME ->
                    ParquetValueWriters.ints(desc)
                io.debezium.time.Time.SCHEMA_NAME ->
                    KafkaParquetWriters.timeAsInt(MILLIS, logicalType.timePrecision(), desc)
                io.debezium.time.MicroTime.SCHEMA_NAME ->
                    KafkaParquetWriters.timeAsLong(MICROS, logicalType.timePrecision(), desc)
                io.debezium.time.NanoTime.SCHEMA_NAME ->
                    KafkaParquetWriters.timeAsLong(NANOS, logicalType.timePrecision(), desc)
                io.debezium.time.ZonedTime.SCHEMA_NAME ->
                    KafkaParquetWriters.zonedTimeAsString(logicalType.timePrecision(), desc)
                io.debezium.time.Timestamp.SCHEMA_NAME ->
                    KafkaParquetWriters.timestampAsLong(MILLIS, logicalType.timePrecision(), desc)
                io.debezium.time.MicroTimestamp.SCHEMA_NAME ->
                    KafkaParquetWriters.timestampAsLong(MICROS, logicalType.timePrecision(), desc)
                io.debezium.time.NanoTimestamp.SCHEMA_NAME ->
                    KafkaParquetWriters.timestampAsLong(NANOS, logicalType.timePrecision(), desc)
                io.debezium.time.ZonedTimestamp.SCHEMA_NAME ->
                    KafkaParquetWriters.zonedTimestampAsString(logicalType.timePrecision(), desc)
                io.debezium.time.Year.SCHEMA_NAME ->
                    ParquetValueWriters.ints(desc)
                io.debezium.data.EnumSet.LOGICAL_NAME -> TODO()
                io.debezium.data.Enum.LOGICAL_NAME ->
                    ParquetValueWriters.strings(desc)

                /////////////// Kafka Logical Types ///////////////
                org.apache.kafka.connect.data.Decimal.LOGICAL_NAME -> {
                    val decimal = logicalType as LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
                    when (type.asPrimitiveType().primitiveTypeName) {
                        INT32 -> ParquetValueWriters.decimalAsInteger(desc, decimal.precision, decimal.scale)
                        INT64 -> ParquetValueWriters.decimalAsLong(desc, decimal.precision, decimal.scale)
                        BINARY,
                        FIXED_LEN_BYTE_ARRAY -> ParquetValueWriters.decimalAsFixed(
                            desc, decimal.precision, decimal.scale
                        )
                        else -> throw UnsupportedOperationException(
                            "Unsupported base type for decimal: ${type.asPrimitiveType().primitiveTypeName}"
                        )
                    }
                }
                org.apache.kafka.connect.data.Date.LOGICAL_NAME ->
                    KafkaParquetWriters.date(desc)
                org.apache.kafka.connect.data.Time.LOGICAL_NAME ->
                    KafkaParquetWriters.timeAsDate(logicalType.timePrecision(), desc)
                org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME ->
                    KafkaParquetWriters.timestampAsDate(logicalType.timePrecision(), desc)
                else -> null
            }
        }
    }
}
