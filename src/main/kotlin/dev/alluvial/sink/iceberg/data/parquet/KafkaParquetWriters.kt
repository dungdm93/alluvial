package dev.alluvial.sink.iceberg.data.parquet

import dev.alluvial.utils.OffsetTimes
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.MILLIS
import dev.alluvial.utils.ZonedDateTimes
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.api.Binary
import java.nio.ByteBuffer
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.Date
import java.util.concurrent.TimeUnit
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

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

    fun timeAsDate(precision: TimePrecision, desc: ColumnDescriptor): ParquetValueWriter<Date> {
        return TimeAsDateWriter(precision, desc)
    }

    fun timeAsInt(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ): ParquetValueWriters.PrimitiveWriter<Int> {
        return TimeAsIntWriter(sourcePrecision, targetPrecision, desc)
    }

    fun timeAsLong(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ): ParquetValueWriters.PrimitiveWriter<Long> {
        return TimeAsLongWriter(sourcePrecision, targetPrecision, desc)
    }

    fun zonedTimeAsString(
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ): ParquetValueWriters.PrimitiveWriter<String> {
        return ZonedTimeAsStringWriter(targetPrecision, desc)
    }

    fun timestampAsDate(precision: TimePrecision, desc: ColumnDescriptor): ParquetValueWriter<Date> {
        return TimestampAsDateWriter(precision, desc)
    }

    fun timestampAsLong(sourcePrecision: TimePrecision, targetPrecision: TimePrecision, desc: ColumnDescriptor):
        ParquetValueWriters.PrimitiveWriter<Long> {
        return TimestampAsLongWriter(sourcePrecision, targetPrecision, desc)
    }

    fun zonedTimestampAsString(
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ): ParquetValueWriters.PrimitiveWriter<String> {
        return ZonedTimestampAsStringWriter(targetPrecision, desc)
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

    private class DateWriter(desc: ColumnDescriptor) :
        ParquetValueWriters.PrimitiveWriter<Date>(desc) {
        override fun write(repetitionLevel: Int, date: Date) {
            val days = TimeUnit.MILLISECONDS.toDays(date.time).toInt()
            column.writeInteger(repetitionLevel, days)
        }
    }

    abstract class TimeWriter<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetValueWriters.PrimitiveWriter<T>(desc) {

        override fun write(repetitionLevel: Int, time: T) {
            val convertedTime = targetPrecision.floorConvert(serialize(time), sourcePrecision)
            if (targetPrecision == MILLIS)
                column.writeInteger(repetitionLevel, convertedTime.toInt()) else
                column.writeLong(repetitionLevel, convertedTime)
        }

        abstract fun serialize(time: T): Long
    }

    private class TimeAsDateWriter(
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : TimeWriter<Date>(MILLIS, targetPrecision, desc) {
        override fun serialize(time: Date) = time.time
    }

    private class TimeAsIntWriter(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : TimeWriter<Int>(sourcePrecision, targetPrecision, desc) {
        override fun serialize(time: Int) = time.toLong()
    }

    private class TimeAsLongWriter(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : TimeWriter<Long>(sourcePrecision, targetPrecision, desc) {
        override fun serialize(time: Long) = time
    }

    private class ZonedTimeAsStringWriter(targetPrecision: TimePrecision, desc: ColumnDescriptor) :
        TimeWriter<String>(targetPrecision, targetPrecision, desc) {
        override fun serialize(time: String): Long {
            val ot = OffsetTime.parse(time)
            assert(ot.offset == ZoneOffset.UTC) { "ZonedTime must be in UTC, got $time" }
            return OffsetTimes.toUtcMidnightTime(ot, targetPrecision)
        }
    }

    abstract class TimestampWriter<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetValueWriters.PrimitiveWriter<T>(desc) {
        override fun write(repetitionLevel: Int, time: T) {
            val convertedTime = targetPrecision.floorConvert(serialize(time), sourcePrecision)
            column.writeLong(repetitionLevel, convertedTime)
        }

        abstract fun serialize(ts: T): Long
    }

    private class TimestampAsDateWriter(
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : TimestampWriter<Date>(MILLIS, targetPrecision, desc) {
        override fun serialize(ts: Date) = ts.time
    }

    private class TimestampAsLongWriter(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : TimestampWriter<Long>(sourcePrecision, targetPrecision, desc) {
        override fun serialize(ts: Long) = ts
    }

    private class ZonedTimestampAsStringWriter(
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : TimestampWriter<String>(targetPrecision, targetPrecision, desc) {
        override fun serialize(ts: String): Long = when (ts.lowercase()) {
            "infinity" -> Long.MAX_VALUE
            "-infinity" -> Long.MIN_VALUE
            else -> {
                val zdt = ZonedDateTime.parse(ts)
                ZonedDateTimes.toEpochTime(zdt, targetPrecision)
            }
        }
    }
}
