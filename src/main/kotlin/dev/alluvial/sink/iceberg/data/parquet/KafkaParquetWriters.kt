package dev.alluvial.sink.iceberg.data.parquet

import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import java.nio.ByteBuffer
import java.util.Date
import java.util.concurrent.TimeUnit
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit as ParquetTimeUnit

object KafkaParquetWriters {
    fun struct(writers: List<ParquetValueWriter<*>?>, schema: KafkaSchema): ParquetValueWriter<KafkaStruct> {
        return StructWriter(writers, schema)
    }

    fun bytes(desc: ColumnDescriptor): ParquetValueWriter<Any> {
        return BytesWriter(desc)
    }

    fun date(desc: ColumnDescriptor): ParquetValueWriter<Date> {
        return DateWriter(desc)
    }

    fun time(desc: ColumnDescriptor, logicalType: TimeLogicalTypeAnnotation): ParquetValueWriter<Date> {
        return TimeWriter(desc, logicalType.unit)
    }

    fun timestamp(desc: ColumnDescriptor, logicalType: TimestampLogicalTypeAnnotation): ParquetValueWriter<Date> {
        return TimestampWriter(desc, logicalType.unit)
    }

    private class StructWriter(writers: List<ParquetValueWriter<*>?>, private val schema: KafkaSchema) :
        ParquetValueWriters.StructWriter<KafkaStruct>(writers) {
        override fun get(struct: KafkaStruct, index: Int): Any? {
            val field = schema.fields()[index]
            return struct.get(field)
        }
    }

    private class BytesWriter(desc: ColumnDescriptor) :
        ParquetValueWriters.PrimitiveWriter<Any>(desc) {
        override fun write(repetitionLevel: Int, data: Any) {
            when (data) {
                is ByteArray -> column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(data))
                is ByteBuffer -> column.writeBinary(repetitionLevel, Binary.fromReusedByteBuffer(data))
                else -> throw IllegalArgumentException("Unsupported write data with type ${data.javaClass}")
            }
        }
    }

    private val TIME_UNIT_MAP = mapOf(
        ParquetTimeUnit.MILLIS to TimeUnit.MILLISECONDS,
        ParquetTimeUnit.MICROS to TimeUnit.MICROSECONDS,
        ParquetTimeUnit.NANOS to TimeUnit.NANOSECONDS,
    )

    private class DateWriter(desc: ColumnDescriptor) :
        ParquetValueWriters.PrimitiveWriter<Date>(desc) {
        override fun write(repetitionLevel: Int, date: Date) {
            val days = TimeUnit.MILLISECONDS.toDays(date.time).toInt()
            column.writeInteger(repetitionLevel, days)
        }
    }

    private class TimeWriter(desc: ColumnDescriptor, timeUnit: ParquetTimeUnit) :
        ParquetValueWriters.PrimitiveWriter<Date>(desc) {
        private val targetUnit = TIME_UNIT_MAP[timeUnit]!!

        override fun write(repetitionLevel: Int, date: Date) {
            val time = targetUnit.convert(date.time, TimeUnit.MILLISECONDS)
            column.writeLong(repetitionLevel, time)
        }
    }

    private class TimestampWriter(desc: ColumnDescriptor, timeUnit: ParquetTimeUnit) :
        ParquetValueWriters.PrimitiveWriter<Date>(desc) {
        private val targetUnit = TIME_UNIT_MAP[timeUnit]!!

        override fun write(repetitionLevel: Int, date: Date) {
            val time = targetUnit.convert(date.time, TimeUnit.MILLISECONDS)
            column.writeLong(repetitionLevel, time)
        }
    }
}
