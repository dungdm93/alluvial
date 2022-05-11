package dev.alluvial.sink.iceberg.type.debezium

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
import dev.alluvial.utils.TimePrecision.MICROS
import dev.alluvial.utils.TimePrecision.NANOS
import dev.alluvial.utils.timePrecision
import org.apache.iceberg.types.Types.TimeType
import org.apache.parquet.column.ColumnDescriptor
import java.util.function.Supplier

internal object NanoTimeConverter : LogicalTypeConverter<Long, Long> {
    override val name = io.debezium.time.NanoTime.SCHEMA_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaAvroReaders.TimeReader<Long>(sourcePrecision, NANOS) {
        override fun deserialize(time: Long, reuse: Any?): Long = time
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaAvroWriters.TimeWriter<Long>(NANOS, targetPrecision) {
        override fun serialize(time: Long): Long = time
    }

    private class ParquetReader(sourcePrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetReaders.TimeReader<Long>(sourcePrecision, NANOS, desc) {
        override fun deserialize(time: Long, reuse: Any?): Long = time
    }

    private class ParquetWriter(targetPrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetWriters.TimeWriter<Long>(NANOS, targetPrecision, desc) {
        override fun serialize(time: Long): Long = time
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType = TimeType.get()

    override fun toIcebergValue(sValue: Long): Long = MICROS.convert(sValue, NANOS)

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<Long> {
        val logicalType = schema.logicalType
        return AvroReader(logicalType.timePrecision())
    }

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<Long> {
        val logicalType = schema.logicalType
        return AvroWriter(logicalType.timePrecision())
    }

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Long> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetReader(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Long> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetWriter(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<Long> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<Long> {
        TODO("Not yet implemented")
    }
}
