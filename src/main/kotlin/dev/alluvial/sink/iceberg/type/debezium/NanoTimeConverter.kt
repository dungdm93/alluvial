package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.avro.KafkaValueReaders
import dev.alluvial.sink.iceberg.avro.KafkaValueWriters
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveWriterContext
import dev.alluvial.sink.iceberg.parquet.KafkaParquetReaders
import dev.alluvial.sink.iceberg.parquet.KafkaParquetWriters
import dev.alluvial.utils.TimePrecision.*
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.timePrecision
import org.apache.iceberg.avro.ValueReader
import org.apache.iceberg.avro.ValueWriter
import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import org.apache.kafka.connect.data.Schema
import org.apache.orc.TypeDescription
import org.apache.parquet.column.ColumnDescriptor
import java.util.function.Supplier

internal object NanoTimeConverter: LogicalTypeConverter<Long, Long> {
    override val name = io.debezium.time.NanoTime.SCHEMA_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaValueReaders.TimeReader<Long>(sourcePrecision, NANOS) {
        override fun deserialize(time: Long, reuse: Any?): Long = time
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaValueWriters.TimeWriter<Long>(NANOS, targetPrecision) {
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

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type = Types.TimeType.get()

    override fun toIcebergValue(sValue: Long): Long = MICROS.convert(sValue, NANOS)

    override fun avroReader(sSchema: Schema, schema: org.apache.avro.Schema): ValueReader<Long> {
        val logicalType = schema.logicalType
        return AvroReader(logicalType.timePrecision())
    }

    override fun avroWriter(sSchema: Schema, schema: org.apache.avro.Schema): ValueWriter<Long> {
        val logicalType = schema.logicalType
        return AvroWriter(logicalType.timePrecision())
    }

    override fun parquetReader(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Long> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetReader(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Long> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetWriter(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun orcReader(sSchema: Schema, type: TypeDescription): OrcValueReader<Long> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: Schema, type: TypeDescription): OrcValueWriter<Long> {
        TODO("Not yet implemented")
    }
}
