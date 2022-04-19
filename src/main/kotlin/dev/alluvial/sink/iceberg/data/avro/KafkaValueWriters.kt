package dev.alluvial.sink.iceberg.data.avro

import dev.alluvial.utils.OffsetTimes
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.ZonedDateTimes
import org.apache.avro.io.Encoder
import org.apache.iceberg.avro.ValueWriter
import org.apache.iceberg.avro.ValueWriters
import java.nio.ByteBuffer
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.util.Date
import java.util.concurrent.TimeUnit
import org.apache.kafka.connect.data.Struct as KafkaStruct

object KafkaValueWriters {
    fun struct(writers: List<ValueWriter<*>>, fields: List<String>): ValueWriter<KafkaStruct> {
        return StructWriter(writers, fields)
    }

    fun <E> array(elementWriter: ValueWriter<E>): ValueWriter<List<E>> {
        return ArrayWriter(elementWriter)
    }

    fun <K, V> map(keyWriter: ValueWriter<K>, valueWriter: ValueWriter<V>): ValueWriter<Map<K, V>> {
        return MapWriter(keyWriter, valueWriter)
    }

    fun <K, V> arrayMap(keyWriter: ValueWriter<K>, valueWriter: ValueWriter<V>): ValueWriter<Map<K, V>> {
        return ArrayMapWriter(keyWriter, valueWriter)
    }

    fun date(): ValueWriter<Date> {
        return DateWriter
    }

    fun timeAsDate(precision: TimePrecision): ValueWriter<Date> {
        return TimeAsDateWriter(precision)
    }

    fun timeAsInt(sourcePrecision: TimePrecision, targetPrecision: TimePrecision): ValueWriter<Int> {
        return TimeAsIntWriter(sourcePrecision, targetPrecision)
    }

    fun timeAsLong(sourcePrecision: TimePrecision, targetPrecision: TimePrecision): ValueWriter<Long> {
        return TimeAsLongWriter(sourcePrecision, targetPrecision)
    }

    fun zonedTimeAsString(targetPrecision: TimePrecision): ValueWriter<String> {
        return ZonedTimeAsStringWriter(targetPrecision)
    }

    fun timestampAsDate(precision: TimePrecision): ValueWriter<Date> {
        return TimestampAsDateWriter(precision)
    }

    fun timestampAsLong(sourcePrecision: TimePrecision, targetPrecision: TimePrecision): ValueWriter<Long> {
        return TimestampAsLongWriter(sourcePrecision, targetPrecision)
    }

    fun zonedTimestampAsString(targetPrecision: TimePrecision): ValueWriter<String> {
        return ZonedTimestampAsStringWriter(targetPrecision)
    }

    fun bytes(): ValueWriter<*> {
        return BytesWriter
    }

    fun arrayAsString(): ValueWriter<String> {
        return ArrayAsStringWriter
    }

    private class StructWriter(writers: List<ValueWriter<*>>, fields: List<String>) : ValueWriter<KafkaStruct> {
        private val writers: List<Pair<String, ValueWriter<*>>>

        init {
            assert(writers.size == fields.size) { "writers.size and fields.size must be equal" }
            this.writers = fields.zip(writers)
        }

        override fun write(struct: KafkaStruct, encoder: Encoder) {
            writers.forEach { (field, writer) ->
                writeField(struct, field, writer, encoder)
            }
        }

        private fun <T> writeField(struct: KafkaStruct, field: String, writer: ValueWriter<T>, encoder: Encoder) {
            @Suppress("UNCHECKED_CAST")
            writer.write(struct.get(field) as T, encoder)
        }
    }

    private class ArrayWriter<E>(
        private val elementWriter: ValueWriter<E>
    ) : ValueWriter<List<E>> {
        override fun write(array: List<E>, encoder: Encoder) {
            encoder.writeArrayStart()
            val numElements: Int = array.size
            encoder.setItemCount(numElements.toLong())
            array.forEach {
                encoder.startItem()
                elementWriter.write(it, encoder)
            }
            encoder.writeArrayEnd()
        }
    }

    private class MapWriter<K, V>(
        private val keyWriter: ValueWriter<K>,
        private val valueWriter: ValueWriter<V>,
    ) : ValueWriter<Map<K, V>> {
        override fun write(map: Map<K, V>, encoder: Encoder) {
            encoder.writeMapStart()
            val numElements: Int = map.size
            encoder.setItemCount(numElements.toLong())
            map.forEach { (k, v) ->
                encoder.startItem()
                keyWriter.write(k, encoder)
                valueWriter.write(v, encoder)
            }
            encoder.writeMapEnd()
        }
    }

    private class ArrayMapWriter<K, V>(
        private val keyWriter: ValueWriter<K>,
        private val valueWriter: ValueWriter<V>,
    ) : ValueWriter<Map<K, V>> {
        override fun write(map: Map<K, V>, encoder: Encoder) {
            encoder.writeArrayStart()
            val numElements: Int = map.size
            encoder.setItemCount(numElements.toLong())
            map.forEach { (k, v) ->
                encoder.startItem()
                keyWriter.write(k, encoder)
                valueWriter.write(v, encoder)
            }
            encoder.writeArrayEnd()
        }
    }

    private object DateWriter : ValueWriter<Date> {
        override fun write(date: Date, encoder: Encoder) {
            val days = TimeUnit.MILLISECONDS.toDays(date.time).toInt()
            encoder.writeInt(days)
        }
    }

    abstract class TimeWriter<T>(
        private val sourcePrecision: TimePrecision,
        private val targetPrecision: TimePrecision,
    ) : ValueWriter<T> {
        init {
            if (targetPrecision == TimePrecision.NANOS) {
                throw IllegalArgumentException("Avro has no $targetPrecision precision time")
            }
        }

        override fun write(datum: T, encoder: Encoder) {
            var time = serialize(datum)
            time = targetPrecision.floorConvert(time, sourcePrecision)
            if (targetPrecision == TimePrecision.MILLIS)
                encoder.writeInt(time.toInt()) else
                encoder.writeLong(time)
        }

        abstract fun serialize(time: T): Long
    }

    private class TimeAsDateWriter(targetPrecision: TimePrecision) :
        TimeWriter<Date>(TimePrecision.MILLIS, targetPrecision) {
        override fun serialize(time: Date) = time.time
    }

    private class TimeAsIntWriter(sourcePrecision: TimePrecision, targetPrecision: TimePrecision) :
        TimeWriter<Int>(sourcePrecision, targetPrecision) {
        override fun serialize(time: Int) = time.toLong()
    }

    private class TimeAsLongWriter(sourcePrecision: TimePrecision, targetPrecision: TimePrecision) :
        TimeWriter<Long>(sourcePrecision, targetPrecision) {
        override fun serialize(time: Long) = time
    }

    private class ZonedTimeAsStringWriter(targetPrecision: TimePrecision) :
        TimeWriter<String>(TimePrecision.NANOS, targetPrecision) {
        override fun serialize(time: String): Long {
            val ot = OffsetTime.parse(time)
            return OffsetTimes.toNanoOfDay(ot)
        }
    }

    abstract class TimestampWriter<T>(
        private val sourcePrecision: TimePrecision,
        private val targetPrecision: TimePrecision,
    ) : ValueWriter<T> {
        init {
            if (targetPrecision == TimePrecision.NANOS) {
                throw IllegalArgumentException("Avro has no $targetPrecision precision timestamp")
            }
        }

        override fun write(ts: T, encoder: Encoder) {
            var time = serialize(ts)
            time = targetPrecision.floorConvert(time, sourcePrecision)
            encoder.writeLong(time)
        }

        abstract fun serialize(ts: T): Long
    }

    private class TimestampAsDateWriter(targetPrecision: TimePrecision) :
        TimestampWriter<Date>(TimePrecision.MILLIS, targetPrecision) {
        override fun serialize(ts: Date) = ts.time
    }

    private class TimestampAsLongWriter(sourcePrecision: TimePrecision, targetPrecision: TimePrecision) :
        TimestampWriter<Long>(sourcePrecision, targetPrecision) {
        override fun serialize(ts: Long) = ts
    }

    private class ZonedTimestampAsStringWriter(targetPrecision: TimePrecision) :
        TimestampWriter<String>(TimePrecision.NANOS, targetPrecision) {
        override fun serialize(ts: String): Long {
            val zdt = ZonedDateTime.parse(ts)
            return ZonedDateTimes.toEpochNano(zdt)
        }
    }

    private object ArrayAsStringWriter : ValueWriter<String> {
        private val delegatedWriter = ValueWriters.array(ValueWriters.strings())

        override fun write(enumStr: String, encoder: Encoder) {
            val enumSet = if (enumStr.isEmpty())
                emptyList() else
                enumStr.split(",")

            delegatedWriter.write(enumSet, encoder)
        }
    }

    private object BytesWriter : ValueWriter<Any> {
        private val byteArrayWriter = ValueWriters.bytes()
        private val byteBufferWriter = ValueWriters.byteBuffers()

        override fun write(datum: Any, encoder: Encoder) {
            when (datum) {
                is ByteArray -> byteArrayWriter.write(datum, encoder)
                is ByteBuffer -> byteBufferWriter.write(datum, encoder)
                else -> throw IllegalArgumentException("Unsupported write data with type ${datum.javaClass}")
            }
        }
    }
}
