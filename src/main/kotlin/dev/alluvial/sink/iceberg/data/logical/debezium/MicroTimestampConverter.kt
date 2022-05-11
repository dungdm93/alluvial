package dev.alluvial.sink.iceberg.data.logical.debezium

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
import dev.alluvial.utils.TimePrecision.MICROS
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

internal object MicroTimestampConverter : LogicalTypeConverter<Long, Long> {
    override val name = io.debezium.time.MicroTimestamp.SCHEMA_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaValueReaders.TimestampReader<Long>(sourcePrecision, MICROS) {
        override fun deserialize(ts: Long, reuse: Any?): Long = ts
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaValueWriters.TimestampWriter<Long>(MICROS, targetPrecision) {
        override fun serialize(ts: Long): Long = ts
    }

    private class ParquetReader(sourcePrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetReaders.TimestampReader<Long>(sourcePrecision, MICROS, desc) {
        override fun deserialize(ts: Long, reuse: Any?): Long = ts
    }

    private class ParquetWriter(targetPrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetWriters.TimestampWriter<Long>(MICROS, targetPrecision, desc) {
        override fun serialize(ts: Long): Long = ts
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type = Types.TimestampType.withoutZone()

    override fun toIcebergValue(sValue: Long): Long = sValue

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
