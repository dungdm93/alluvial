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
import dev.alluvial.utils.TimePrecision.MILLIS
import dev.alluvial.utils.timePrecision
import org.apache.iceberg.types.Types.TimeType
import org.apache.parquet.column.ColumnDescriptor
import java.util.function.Supplier

internal object TimeConverter : LogicalTypeConverter<Int, Long> {
    override val name = io.debezium.time.Time.SCHEMA_NAME

    private class AvroTimeReader(sourcePrecision: TimePrecision) :
        KafkaAvroReaders.TimeReader<Int>(sourcePrecision, MILLIS) {
        override fun deserialize(time: Long, reuse: Any?): Int = time.toInt()
    }

    private class AvroTimeWriter(targetPrecision: TimePrecision) :
        KafkaAvroWriters.TimeWriter<Int>(MILLIS, targetPrecision) {
        override fun serialize(time: Int): Long = time.toLong()
    }

    private class ParquetTimeReader(sourcePrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetReaders.TimeReader<Int>(sourcePrecision, MILLIS, desc) {
        override fun deserialize(time: Long, reuse: Any?): Int = time.toInt()
    }

    private class ParquetTimeWriter(targetPrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetWriters.TimeWriter<Int>(MILLIS, targetPrecision, desc) {
        override fun serialize(time: Int): Long = time.toLong()
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType = TimeType.get()

    override fun toIcebergValue(sValue: Int): Long = MICROS.convert(sValue.toLong(), MILLIS)

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<Int> {
        val logicalType = schema.logicalType
        return AvroTimeReader(logicalType.timePrecision())
    }

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<Int> {
        val logicalType = schema.logicalType
        return AvroTimeWriter(logicalType.timePrecision())
    }

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Int> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetTimeReader(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Int> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetTimeWriter(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<Int> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<Int> {
        TODO("Not yet implemented")
    }
}
