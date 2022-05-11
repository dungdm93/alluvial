package dev.alluvial.sink.iceberg.type.kafka

import dev.alluvial.sink.iceberg.avro.KafkaAvroReaders
import dev.alluvial.sink.iceberg.avro.KafkaAvroWriters
import dev.alluvial.sink.iceberg.parquet.KafkaParquetReaders
import dev.alluvial.sink.iceberg.parquet.KafkaParquetWriters
import dev.alluvial.sink.iceberg.type.AvroSchema
import dev.alluvial.sink.iceberg.type.AvroValueReader
import dev.alluvial.sink.iceberg.type.AvroValueWriter
import dev.alluvial.sink.iceberg.type.IcebergType
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.OrcType
import dev.alluvial.sink.iceberg.type.OrcValueReader
import dev.alluvial.sink.iceberg.type.OrcValueWriter
import dev.alluvial.sink.iceberg.type.ParquetType
import dev.alluvial.sink.iceberg.type.ParquetValueReader
import dev.alluvial.sink.iceberg.type.ParquetValueWriter
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveWriterContext
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.MILLIS
import dev.alluvial.utils.timePrecision
import org.apache.iceberg.types.Types.TimestampType
import org.apache.parquet.column.ColumnDescriptor
import java.util.Date
import java.util.function.Supplier

internal object TimestampConverter : LogicalTypeConverter<Date, Long> {
    override val name = org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaAvroReaders.TimestampReader<Date>(sourcePrecision, MILLIS) {
        override fun deserialize(ts: Long, reuse: Any?): Date {
            return if (ts == (reuse as? Date)?.time)
                reuse else
                Date(ts)
        }
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaAvroWriters.TimestampWriter<Date>(MILLIS, targetPrecision) {
        override fun serialize(ts: Date) = ts.time
    }

    private class ParquetReader(
        sourcePrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : KafkaParquetReaders.TimestampReader<Date>(sourcePrecision, MILLIS, desc) {
        override fun deserialize(ts: Long, reuse: Any?): Date {
            return if (ts == (reuse as? Date)?.time)
                reuse else
                Date(ts)
        }
    }

    private class ParquetWriter(
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : KafkaParquetWriters.TimestampWriter<Date>(MILLIS, targetPrecision, desc) {
        override fun serialize(ts: Date) = ts.time
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType {
        return TimestampType.withoutZone()
    }

    override fun toIcebergValue(sValue: Date): Long = sValue.time

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<Date> {
        val logicalType = schema.logicalType
        return AvroReader(logicalType.timePrecision())
    }

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<Date> {
        val logicalType = schema.logicalType
        return AvroWriter(logicalType.timePrecision())
    }

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Date> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetReader(logicalType.timePrecision(), primitiveCtx.desc)

    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Date> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetWriter(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<Date> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<Date> {
        TODO("Not yet implemented")
    }
}
