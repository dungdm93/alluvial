package dev.alluvial.sink.iceberg.data.logical.kafka

import dev.alluvial.sink.iceberg.data.avro.KafkaValueReaders
import dev.alluvial.sink.iceberg.data.avro.KafkaValueWriters
import dev.alluvial.sink.iceberg.data.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.data.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.data.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.data.logical.ParquetWriterContext
import dev.alluvial.sink.iceberg.data.logical.ParquetPrimitiveWriterContext
import dev.alluvial.sink.iceberg.data.parquet.KafkaParquetReaders
import dev.alluvial.sink.iceberg.data.parquet.KafkaParquetWriters
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.MILLIS
import dev.alluvial.utils.timePrecision
import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.types.Types.TimeType
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

internal object TimeConverter : LogicalTypeConverter<Date, Long> {
    override val name = org.apache.kafka.connect.data.Time.LOGICAL_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaValueReaders.TimeReader<Date>(sourcePrecision, MILLIS) {
        override fun deserialize(time: Long, reuse: Any?): Date {
            return if (time == (reuse as? Date)?.time)
                reuse else
                Date(time)
        }
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaValueWriters.TimeWriter<Date>(MILLIS, targetPrecision) {
        override fun serialize(time: Date) = time.time
    }

    private class ParquetReader(
        sourcePrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : KafkaParquetReaders.TimeReader<Date>(sourcePrecision, MILLIS, desc) {
        override fun deserialize(time: Long, reuse: Any?): Date {
            return if (time == (reuse as? Date)?.time)
                reuse else
                Date(time)
        }
    }

    private class ParquetWriter(
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : KafkaParquetWriters.TimeWriter<Date>(MILLIS, targetPrecision, desc) {
        override fun serialize(time: Date) = time.time
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType {
        return TimeType.get()
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
