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
import dev.alluvial.utils.ZonedDateTimes
import dev.alluvial.utils.timePrecision
import io.debezium.time.ZonedTimestamp
import org.apache.iceberg.types.Types.TimestampType
import org.apache.parquet.column.ColumnDescriptor
import java.time.ZonedDateTime
import java.util.function.Supplier

internal object ZonedTimestampConverter : LogicalTypeConverter<String, Long> {
    @Suppress("RemoveRedundantQualifierName")
    override val name = io.debezium.time.ZonedTimestamp.SCHEMA_NAME

    private class AvroReader(sourcePrecision: TimePrecision) :
        KafkaAvroReaders.TimestampReader<String>(sourcePrecision, sourcePrecision) {
        override fun deserialize(ts: Long, reuse: Any?): String = when (ts) {
            Long.MAX_VALUE -> "infinity"
            Long.MIN_VALUE -> "-infinity"
            else -> {
                val zdt = ZonedDateTimes.ofEpochTime(ts, sourcePrecision)
                ZonedTimestamp.toIsoString(zdt, null, null)
            }
        }
    }

    private class AvroWriter(targetPrecision: TimePrecision) :
        KafkaAvroWriters.TimestampWriter<String>(targetPrecision, targetPrecision) {
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
                ZonedTimestamp.toIsoString(zdt, null, null)
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

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType = TimestampType.withZone()

    override fun toIcebergValue(sValue: String): Long {
        val zdt = ZonedDateTime.parse(sValue)
        return ZonedDateTimes.toEpochTime(zdt, TimePrecision.MICROS)
    }

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<String> {
        val logicalType = schema.logicalType
        return AvroReader(logicalType.timePrecision())
    }

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<String> {
        val logicalType = schema.logicalType
        return AvroWriter(logicalType.timePrecision())
    }

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<String> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetReader(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<String> {
        val logicalType = type.logicalTypeAnnotation
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetWriter(logicalType.timePrecision(), primitiveCtx.desc)
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<String> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<String> {
        TODO("Not yet implemented")
    }
}
