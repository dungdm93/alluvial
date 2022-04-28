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
import dev.alluvial.utils.OffsetTimes
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.timePrecision
import io.debezium.time.ZonedTime
import org.apache.iceberg.avro.ValueReader
import org.apache.iceberg.avro.ValueWriter
import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types.TimeType
import org.apache.kafka.connect.data.Schema
import org.apache.orc.TypeDescription
import org.apache.parquet.column.ColumnDescriptor
import java.time.OffsetTime
import java.time.ZoneOffset
import java.util.function.Supplier

internal object ZonedTimeConverter : LogicalTypeConverter<String, Long> {
    override val name = ZonedTime.SCHEMA_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaValueReaders.TimeReader<String>(sourcePrecision, sourcePrecision) {
        override fun deserialize(time: Long, reuse: Any?): String {
            val offsetTime = OffsetTimes.ofUtcMidnightTime(time, sourcePrecision)
            return ZonedTime.toIsoString(offsetTime, null)
        }
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaValueWriters.TimeWriter<String>(targetPrecision, targetPrecision) {
        override fun serialize(time: String): Long {
            val ot = OffsetTime.parse(time)
            assert(ot.offset == ZoneOffset.UTC) { "ZonedTime must be in UTC, got $time" }
            return OffsetTimes.toUtcMidnightTime(ot, targetPrecision)
        }
    }

    private class ParquetReader(sourcePrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetReaders.TimeReader<String>(sourcePrecision, sourcePrecision, desc) {
        override fun deserialize(time: Long, reuse: Any?): String {
            val offsetTime = OffsetTimes.ofUtcMidnightTime(time, sourcePrecision)
            return ZonedTime.toIsoString(offsetTime, null)
        }
    }

    private class ParquetWriter(targetPrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetWriters.TimeWriter<String>(targetPrecision, targetPrecision, desc) {
        override fun serialize(time: String): Long {
            val ot = OffsetTime.parse(time)
            assert(ot.offset == ZoneOffset.UTC) { "ZonedTime must be in UTC, got $time" }
            return OffsetTimes.toUtcMidnightTime(ot, targetPrecision)
        }
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type = TimeType.get()

    override fun toIcebergValue(sValue: String): Long {
        val ot = OffsetTime.parse(sValue)
        assert(ot.offset == ZoneOffset.UTC) { "ZonedTime must be in UTC, got $sValue" }
        return OffsetTimes.toUtcMidnightTime(ot, TimePrecision.MICROS)
    }

    override fun avroReader(sSchema: Schema, schema: org.apache.avro.Schema): ValueReader<String> {
        val logicalType = schema.logicalType
        return AvroReader(logicalType.timePrecision())
    }

    override fun avroWriter(sSchema: Schema, schema: org.apache.avro.Schema): ValueWriter<String> {
        val logicalType = schema.logicalType
        return AvroWriter(logicalType.timePrecision())
    }

    override fun parquetReader(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetReaderContext
    ): ParquetValueReader<String> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetReader(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<String> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetWriter(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun orcReader(sSchema: Schema, type: TypeDescription): OrcValueReader<String> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: Schema, type: TypeDescription): OrcValueWriter<String> {
        TODO("Not yet implemented")
    }
}
