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
import dev.alluvial.utils.ZonedDateTimes
import dev.alluvial.utils.timePrecision
import io.debezium.time.ZonedTimestamp
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
import java.time.ZonedDateTime
import java.util.function.Supplier

internal object ZonedTimestampConverter : LogicalTypeConverter<String, Long> {
    override val name = ZonedTimestamp.SCHEMA_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaValueReaders.TimestampReader<String>(sourcePrecision, sourcePrecision) {
        override fun deserialize(ts: Long, reuse: Any?): String = when (ts) {
            Long.MAX_VALUE -> "infinity"
            Long.MIN_VALUE -> "-infinity"
            else -> {
                val zdt = ZonedDateTimes.ofEpochTime(ts, sourcePrecision)
                ZonedTimestamp.toIsoString(zdt, null)
            }
        }
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaValueWriters.TimestampWriter<String>(targetPrecision, targetPrecision) {
        override fun serialize(ts: String): Long = when (ts.lowercase()) {
            "infinity" -> Long.MAX_VALUE
            "-infinity" -> Long.MIN_VALUE
            else -> {
                val zdt = ZonedDateTime.parse(ts)
                ZonedDateTimes.toEpochTime(zdt, targetPrecision)
            }
        }
    }


    private class ParquetReader(sourcePrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetReaders.TimestampReader<String>(sourcePrecision, sourcePrecision, desc) {
        override fun deserialize(ts: Long, reuse: Any?): String = when (ts) {
            Long.MAX_VALUE -> "infinity"
            Long.MIN_VALUE -> "-infinity"
            else -> {
                val zdt = ZonedDateTimes.ofEpochTime(ts, sourcePrecision)
                ZonedTimestamp.toIsoString(zdt, null)
            }
        }
    }

    private class ParquetWriter(targetPrecision: TimePrecision, desc: ColumnDescriptor) :
        KafkaParquetWriters.TimestampWriter<String>(targetPrecision, targetPrecision, desc) {
        override fun serialize(ts: String): Long = when (ts.lowercase()) {
            "infinity" -> Long.MAX_VALUE
            "-infinity" -> Long.MIN_VALUE
            else -> {
                val zdt = ZonedDateTime.parse(ts)
                ZonedDateTimes.toEpochTime(zdt, targetPrecision)
            }
        }
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type = Types.TimestampType.withZone()

    override fun toIcebergValue(sValue: String): Long {
        val zdt = ZonedDateTime.parse(sValue)
        return ZonedDateTimes.toEpochTime(zdt, TimePrecision.MICROS)
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
