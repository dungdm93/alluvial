package dev.alluvial.sink.iceberg.data.parquet

import dev.alluvial.utils.OffsetTimes
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.ZonedDateTimes
import io.debezium.time.ZonedTime
import io.debezium.time.ZonedTimestamp
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.iceberg.parquet.ParquetValueReaders.PrimitiveReader
import org.apache.iceberg.parquet.ParquetValueReaders.UnboxedReader
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.Type
import java.util.Date
import java.util.concurrent.TimeUnit
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

object KafkaParquetReaders {
    fun strings(desc: ColumnDescriptor): ParquetValueReader<String> {
        return ParquetValueReaders.StringReader(desc)
    }

    fun byteArrays(desc: ColumnDescriptor): ParquetValueReader<ByteArray> {
        return ParquetValueReaders.ByteArrayReader(desc)
    }

    fun <T> unboxed(desc: ColumnDescriptor): ParquetValueReader<T> {
        return UnboxedReader(desc)
    }

    fun tinyints(desc: ColumnDescriptor): ParquetValueReader<Byte> {
        return ByteReader(desc)
    }

    fun shorts(desc: ColumnDescriptor): ParquetValueReader<Short> {
        return ShortReader(desc)
    }

    fun list(
        definitionLevel: Int,
        repetitionLevel: Int,
        elementReader: ParquetValueReader<*>
    ): ParquetValueReader<out List<*>> {
        return ParquetValueReaders.ListReader(definitionLevel, repetitionLevel, elementReader)
    }

    fun map(
        definitionLevel: Int,
        repetitionLevel: Int,
        keyWriter: ParquetValueReader<*>,
        valueWriter: ParquetValueReader<*>
    ): ParquetValueReader<out Map<*, *>> {
        return ParquetValueReaders.MapReader(definitionLevel, repetitionLevel, keyWriter, valueWriter)
    }

    fun struct(
        types: List<Type>,
        fieldReaders: List<ParquetValueReader<*>?>,
        struct: KafkaSchema,
    ): ParquetValueReader<*> {
        return StructReader(types, fieldReaders, struct)
    }

    fun date(desc: ColumnDescriptor): ParquetValueReader<Date> {
        return DateReader(desc)
    }

    fun timeAsDate(precision: TimePrecision, desc: ColumnDescriptor): ParquetValueReader<Date> {
        return TimeAsDateReader(precision, desc)
    }

    fun timeAsInt(sourcePrecision: TimePrecision, targetPrecision: TimePrecision, desc: ColumnDescriptor):
        ParquetValueReader<Int> {
        return TimeAsIntReader(sourcePrecision, targetPrecision, desc)
    }

    fun timeAsLong(sourcePrecision: TimePrecision, targetPrecision: TimePrecision, desc: ColumnDescriptor):
        ParquetValueReader<Long> {
        return TimeAsLongReader(sourcePrecision, targetPrecision, desc)
    }

    fun zonedTimeAsString(
        timePrecision: TimePrecision,
        desc: ColumnDescriptor
    ): ParquetValueReader<String> {
        return ZonedTimeAsStringReader(timePrecision, desc)
    }

    fun timestampAsDate(precision: TimePrecision, desc: ColumnDescriptor): ParquetValueReader<Date> {
        return TimestampAsDateReader(precision, desc)
    }

    fun timestampAsLong(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ): ParquetValueReader<Long> {
        return TimestampAsLongReader(sourcePrecision, targetPrecision, desc)
    }

    fun zonedTimestampAsString(
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ): ParquetValueReader<String> {
        return ZonedTimestampAsStringReader(targetPrecision, desc)
    }

    private class StructReader(
        types: List<Type>,
        readers: List<ParquetValueReader<*>?>,
        private val kafkaSchema: KafkaSchema,
    ) : ParquetValueReaders.StructReader<KafkaStruct, KafkaStruct>(types, readers) {
        override fun newStructData(reuse: KafkaStruct?): KafkaStruct {
            return if (reuse is KafkaStruct)
                reuse else
                KafkaStruct(kafkaSchema)
        }

        override fun buildStruct(struct: KafkaStruct): KafkaStruct = struct

        override fun getField(intermediate: KafkaStruct, pos: Int): Any? {
            val field = kafkaSchema.fields()[pos]
            return intermediate.get(field)
        }

        override fun set(struct: KafkaStruct, pos: Int, value: Any?) {
            val field = kafkaSchema.fields()[pos]
            struct.put(field, value)
        }
    }

    private class ShortReader(desc: ColumnDescriptor) : UnboxedReader<Short>(desc) {
        override fun read(ignored: Short?): Short {
            return readInteger().toShort()
        }
    }

    private class ByteReader(desc: ColumnDescriptor) : UnboxedReader<Byte>(desc) {
        override fun read(ignored: Byte?): Byte {
            return readInteger().toByte()
        }
    }

    private class DateReader(desc: ColumnDescriptor) : UnboxedReader<Date>(desc) {
        override fun read(reuse: Date?): Date {
            val days = readInteger().toLong()
            val time = TimeUnit.DAYS.toMillis(days)
            return if (time == reuse?.time) reuse else Date(time)
        }
    }

    abstract class ParquetTimeReader<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : PrimitiveReader<T>(desc) {
        override fun read(reuse: T?): T {
            var time = if (sourcePrecision == TimePrecision.MILLIS)
                column.nextInteger().toLong() else
                column.nextLong()

            time = targetPrecision.floorConvert(time, sourcePrecision)
            return deserialize(time, reuse)
        }

        abstract fun deserialize(time: Long, reuse: Any?): T
    }

    private class TimeAsDateReader(
        sourcePrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetTimeReader<Date>(sourcePrecision, TimePrecision.MILLIS, desc) {
        override fun deserialize(time: Long, reuse: Any?): Date {
            return if (time == (reuse as? Date)?.time)
                reuse else
                Date(time)
        }
    }

    private class TimeAsIntReader(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetTimeReader<Int>(sourcePrecision, targetPrecision, desc) {
        override fun deserialize(time: Long, reuse: Any?): Int = time.toInt()
    }

    private class TimeAsLongReader(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetTimeReader<Long>(sourcePrecision, targetPrecision, desc) {
        override fun deserialize(time: Long, reuse: Any?): Long = time
    }

    private class ZonedTimeAsStringReader(
        sourcePrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetTimeReader<String>(sourcePrecision, sourcePrecision, desc) {
        override fun deserialize(time: Long, reuse: Any?): String {
            val offsetTime = OffsetTimes.ofUtcMidnightTime(time, sourcePrecision)
            return ZonedTime.toIsoString(offsetTime, null)
        }
    }

    abstract class ParquetTimestampReader<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : PrimitiveReader<T>(desc) {
        override fun read(reuse: T?): T {
            var ts = column.nextLong()
            ts = targetPrecision.floorConvert(ts, sourcePrecision)

            return deserialize(ts, reuse)
        }

        abstract fun deserialize(ts: Long, reuse: Any?): T
    }

    private class TimestampAsDateReader(
        sourcePrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetTimestampReader<Date>(sourcePrecision, TimePrecision.MILLIS, desc) {
        override fun deserialize(ts: Long, reuse: Any?): Date {
            return if (ts == (reuse as? Date)?.time)
                reuse else
                Date(ts)
        }
    }

    private class TimestampAsLongReader(
        sourcePrecision: TimePrecision,
        targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetTimestampReader<Long>(sourcePrecision, targetPrecision, desc) {
        override fun deserialize(ts: Long, reuse: Any?) = ts
    }

    private class ZonedTimestampAsStringReader(
        sourcePrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetTimestampReader<String>(sourcePrecision, sourcePrecision, desc) {
        override fun deserialize(ts: Long, reuse: Any?): String = when (ts) {
            Long.MAX_VALUE -> "infinity"
            Long.MIN_VALUE -> "-infinity"
            else -> {
                val zdt = ZonedDateTimes.ofEpochTime(ts, sourcePrecision)
                ZonedTimestamp.toIsoString(zdt, null)
            }
        }
    }
}
