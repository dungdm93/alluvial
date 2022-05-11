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
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.*
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

internal object TimeConverter : LogicalTypeConverter<Int, Long> {
    override val name = io.debezium.time.Time.SCHEMA_NAME

    private class AvroTimeReader(sourcePrecision: TimePrecision) :
        KafkaValueReaders.TimeReader<Int>(sourcePrecision, MILLIS) {
        override fun deserialize(time: Long, reuse: Any?): Int = time.toInt()
    }

    private class AvroTimeWriter(targetPrecision: TimePrecision) :
        KafkaValueWriters.TimeWriter<Int>(MILLIS, targetPrecision) {
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

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type = Types.TimeType.get()

    override fun toIcebergValue(sValue: Int): Long = MICROS.convert(sValue.toLong(), MILLIS)

    override fun avroReader(sSchema: Schema, schema: org.apache.avro.Schema): ValueReader<Int> {
        val logicalType = schema.logicalType
        return AvroTimeReader(logicalType.timePrecision())
    }

    override fun avroWriter(sSchema: Schema, schema: org.apache.avro.Schema): ValueWriter<Int> {
        val logicalType = schema.logicalType
        return AvroTimeWriter(logicalType.timePrecision())
    }

    override fun parquetReader(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Int> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetTimeReader(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Int> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetTimeWriter(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun orcReader(sSchema: Schema, type: TypeDescription): OrcValueReader<Int> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: Schema, type: TypeDescription): OrcValueWriter<Int> {
        TODO("Not yet implemented")
    }
}
