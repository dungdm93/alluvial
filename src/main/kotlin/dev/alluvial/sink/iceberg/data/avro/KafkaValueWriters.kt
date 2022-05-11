package dev.alluvial.sink.iceberg.data.avro

import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.MILLIS
import dev.alluvial.utils.TimePrecision.NANOS
import org.apache.avro.io.Encoder
import org.apache.iceberg.avro.ValueWriter
import org.apache.iceberg.avro.ValueWriters
import java.nio.ByteBuffer
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

    fun bytes(): ValueWriter<*> {
        return BytesWriter
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

    abstract class TimeWriter<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
    ) : ValueWriter<T> {
        init {
            if (targetPrecision == NANOS) {
                throw IllegalArgumentException("Avro has no $targetPrecision precision time")
            }
        }

        override fun write(datum: T, encoder: Encoder) {
            var time = serialize(datum)
            time = targetPrecision.floorConvert(time, sourcePrecision)
            if (targetPrecision == MILLIS)
                encoder.writeInt(time.toInt()) else
                encoder.writeLong(time)
        }

        abstract fun serialize(time: T): Long
    }

    abstract class TimestampWriter<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
    ) : ValueWriter<T> {
        init {
            if (targetPrecision == NANOS) {
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
