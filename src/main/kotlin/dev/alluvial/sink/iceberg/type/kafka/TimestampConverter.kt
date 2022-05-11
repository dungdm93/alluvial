package dev.alluvial.sink.iceberg.type.kafka

import dev.alluvial.sink.iceberg.avro.KafkaValueReaders
import dev.alluvial.sink.iceberg.avro.KafkaValueWriters
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveWriterContext
import dev.alluvial.sink.iceberg.parquet.KafkaParquetReaders
import dev.alluvial.sink.iceberg.parquet.KafkaParquetWriters
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.MILLIS
import dev.alluvial.utils.timePrecision
import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.types.Types.TimestampType
import org.apache.parquet.column.ColumnDescriptor
import java.util.Date
import java.util.function.Supplier
import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.avro.ValueReader as AvroValueReader
import org.apache.iceberg.avro.ValueWriter as AvroValueWriter
import org.apache.iceberg.types.Type as IcebergType
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.orc.TypeDescription as OrcType
import org.apache.parquet.schema.Type as ParquetType

internal object TimestampConverter : LogicalTypeConverter<Date, Long> {
    override val name = org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaValueReaders.TimestampReader<Date>(sourcePrecision, MILLIS) {
        override fun deserialize(ts: Long, reuse: Any?): Date {
            return if (ts == (reuse as? Date)?.time)
                reuse else
                Date(ts)
        }
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaValueWriters.TimestampWriter<Date>(MILLIS, targetPrecision) {
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
